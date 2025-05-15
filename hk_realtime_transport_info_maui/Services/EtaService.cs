using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using hk_realtime_transport_info_maui.Models;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using System.Threading;

namespace hk_realtime_transport_info_maui.Services
{
    public class EtaService
    {
        private readonly HttpClient _httpClient;
        private readonly LiteDbService _databaseService;
        private readonly ILogger<EtaService> _logger;
        private readonly string _kmbStopEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/eta";
        private readonly string _kmbRouteEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta";
        private readonly string _kmbStopSpecificEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/stop-eta";
        
        // Cached JsonSerializerOptions to avoid recreating them on each request
        private readonly JsonSerializerOptions _jsonOptions;
        
        // Enhanced cache service
        private readonly CacheService _cacheService;
        
        // Constants for retry logic
        private const int MaxRetryAttempts = 3;
        private const int RetryDelayMilliseconds = 1000;
        
        // Cache expiration times
        private static readonly TimeSpan EtaCacheExpiration = TimeSpan.FromSeconds(25); // ETAs refresh every 30 seconds

        // Add a semaphore to limit concurrent API requests - effectively remove limit by setting to a large number
        // private readonly SemaphoreSlim _apiRequestSemaphore = new SemaphoreSlim(1000, 1000); // Semaphore no longer used
        
        // Add additional rate limiting
        private readonly object _lastRequestLock = new object();
        private DateTime _lastRequestTime = DateTime.MinValue;
        private const int MinRequestIntervalMs = 300; // Minimum time between API requests

        // Add a priority queue-based ETA fetching approach
        private readonly ConcurrentQueue<EtaFetchRequest> _etaFetchQueue = new ConcurrentQueue<EtaFetchRequest>();
        private readonly SemaphoreSlim _etaQueueSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _processingFlag = new SemaphoreSlim(1, 1);
        private bool _isProcessingQueue = false;

        // Structure to track ETA fetch requests
        private class EtaFetchRequest
        {
            public string StopId { get; set; }
            public string? RouteNumber { get; set; }
            public string? ServiceType { get; set; }
            public int Priority { get; set; } // Lower number = higher priority
            public TaskCompletionSource<List<TransportEta>> CompletionSource { get; set; } = new TaskCompletionSource<List<TransportEta>>();
        }

        // Add these fields at the top of the class, right after the existing fields
        private readonly ConcurrentDictionary<string, Task<List<TransportEta>>> _pendingEtaRequests = 
            new ConcurrentDictionary<string, Task<List<TransportEta>>>();
        private readonly ConcurrentDictionary<string, bool> _stopHasNoEtas = 
            new ConcurrentDictionary<string, bool>();
        private DateTime _lastEtaRequestTime = DateTime.MinValue;

        public EtaService(HttpClient httpClient, LiteDbService databaseService, ILogger<EtaService> logger, CacheService cacheService)
        {
            _httpClient = httpClient;
            _databaseService = databaseService;
            _logger = logger;
            _cacheService = cacheService;
            
            // Initialize the JSON options once
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                NumberHandling = JsonNumberHandling.AllowReadingFromString,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };
        }

        /// <summary>
        /// Applies rate limiting to ensure we don't overwhelm the API
        /// </summary>
        private async Task ApplyRateLimiting()
        {
            DateTime now = DateTime.UtcNow;
            // int delayMs = 0; // Original calculation removed to disable delay from this method's new logic
            
            lock (_lastRequestLock)
            {
                // Set _lastRequestTime to current time to prevent runaway calculations and disable interval-based delay.
                _lastRequestTime = now; 
                }
                
            // Since delayMs is not calculated to be > 0 by this method's new logic, 
            // the Task.Delay will not be hit from here.
            // Original delay logic:
            // if (delayMs > 0)
            // {
            //    _logger?.LogDebug("API rate limiting: delaying request by {delayMs}ms", delayMs);
            //    await Task.Delay(delayMs);
            // }
            await Task.CompletedTask; // Ensure method is async and does no actual delay work.
        }

        /// <summary>
        /// Fetches ETA data for a specific KMB stop with retry logic and caching
        /// </summary>
        public async Task<List<TransportEta>> FetchKmbEtaForStop(string stopId, string routeNumber, string serviceType)
        {
            // Create a cache key
            string cacheKey = $"eta_kmb_{stopId}_{routeNumber}_{serviceType}";
            
            // Try to get from cache first using the enhanced cache service
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved ETAs for KMB stop {stopId} route {routeNumber} from cache", stopId, routeNumber);
                return cachedEtas;
            }
            
            int retryCount = 0;
            while (true)
            {
                try
                {
                    string url = $"{_kmbStopEtaBaseUrl}/{stopId}/{routeNumber}/{serviceType}";
                    _logger?.LogDebug("Fetching KMB stop ETA from: {url}", url);

                    // Use cancellation token to avoid hanging requests
                    using var cts = new CancellationTokenSource();
                    cts.CancelAfter(TimeSpan.FromSeconds(10)); // 10 second timeout
                    
                    var response = await _httpClient.GetAsync(url, cts.Token);
                    
                    // Check for specific status codes
                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        _logger?.LogWarning("KMB stop ETA data not found: {stopId}/{routeNumber}/{serviceType}", 
                            stopId, routeNumber, serviceType);
                        return new List<TransportEta>();
                    }
                    
                    if (!response.IsSuccessStatusCode)
                    {
                        if (retryCount < MaxRetryAttempts)
                        {
                            retryCount++;
                            _logger?.LogWarning("Failed to fetch KMB stop ETA data (attempt {retryCount}/{maxRetries}): {statusCode}", 
                                retryCount, MaxRetryAttempts, response.StatusCode);
                            
                            // Exponential backoff
                            await Task.Delay(RetryDelayMilliseconds * retryCount);
                            continue;
                        }
                        
                        _logger?.LogError("Failed to fetch KMB stop ETA data after {attempts} attempts: {statusCode}", 
                            MaxRetryAttempts, response.StatusCode);
                        return new List<TransportEta>();
                    }

                    // Use ReadAsStreamAsync for better performance with large responses
                    var stream = await response.Content.ReadAsStreamAsync();
                    
                    // Try to deserialize using System.Text.Json
                    try 
                    {
                        var etaResponse = await JsonSerializer.DeserializeAsync<KmbEtaResponse>(stream, _jsonOptions);
                        
                        if (etaResponse?.Data == null || etaResponse.Data.Count == 0)
                        {
                            _logger?.LogWarning("No ETA data found for KMB stop {stopId} route {routeNumber}", stopId, routeNumber);
                            return new List<TransportEta>();
                        }

                        var result = new List<TransportEta>(etaResponse.Data.Count); // Pre-size the list
                        DateTime now = DateTime.Now; // Cache current time
                        
                        foreach (var etaData in etaResponse.Data)
                        {
                            if (string.IsNullOrEmpty(etaData.Eta)) continue;

                            // Check if the stop ID is valid
                            if (string.IsNullOrEmpty(etaData.StopId))
                            {
                                _logger?.LogWarning("Skipping ETA data with null or empty stop ID for KMB stop {stopId} route {routeNumber}", stopId, routeNumber);
                                continue;
                            }

                            // First parse the ETA time since we need it for ID generation
                            if (!DateTime.TryParseExact(etaData.Eta, "yyyy-MM-ddTHH:mm:ss+08:00", 
                                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime etaTime))
                            {
                                _logger?.LogWarning("Failed to parse ETA time: {etaTime}", etaData.Eta);
                                continue;
                            }
                            
                            // Restore the correct parameters
                            string etaId = $"{etaData.Route}_{etaData.StopId}_{etaData.EtaSeq}_{etaData.Dir}_{etaTime:yyyyMMddHHmmss}";

                            // Calculate minutes remaining
                            TimeSpan timeRemaining = etaTime - now;
                            int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                            
                            var eta = new TransportEta
                            {
                                Id = etaId,
                                StopId = etaData.StopId ?? string.Empty, // Ensure StopId is never null
                                RouteId = $"KMB_{etaData.Route}_{serviceType}_{etaData.Dir}", // Restore the correct parameters
                                RouteNumber = etaData.Route ?? string.Empty, // Fix null warning
                                Direction = etaData.Dir ?? string.Empty, // Fix null warning
                                ServiceType = serviceType,
                                FetchTime = now,
                                Remarks = etaData.Remarks ?? string.Empty, // Ensure Remarks is never null
                                EtaTime = etaTime,
                                RemainingMinutes = minutes <= 0 ? "0" : minutes.ToString(),
                                IsCancelled = !string.IsNullOrEmpty(etaData.Remarks) && 
                                              etaData.Remarks?.Contains("cancel", StringComparison.OrdinalIgnoreCase) == true
                            };

                            result.Add(eta);
                        }

                        // Cache the result using our enhanced cache service
                        _cacheService.Set(cacheKey, result, EtaCacheExpiration);
                        
                        _logger?.LogDebug("Successfully fetched and cached {count} ETAs for KMB stop {stopId}", result.Count, stopId);
                        return result;
                    }
                    catch (JsonException ex)
                    {
                        if (retryCount < MaxRetryAttempts)
                        {
                            retryCount++;
                            _logger?.LogWarning(ex, "Error deserializing KMB stop ETA response (attempt {retryCount}/{maxRetries})", 
                                retryCount, MaxRetryAttempts);
                            
                            await Task.Delay(RetryDelayMilliseconds * retryCount);
                            continue;
                        }
                        
                        _logger?.LogError(ex, "Error deserializing KMB stop ETA response after {attempts} attempts", MaxRetryAttempts);
                        return new List<TransportEta>();
                    }
                }
                catch (TaskCanceledException ex)
                {
                    if (retryCount < MaxRetryAttempts)
                    {
                        retryCount++;
                        _logger?.LogWarning(ex, "KMB ETA request timed out (attempt {retryCount}/{maxRetries})", 
                            retryCount, MaxRetryAttempts);
                        
                        await Task.Delay(RetryDelayMilliseconds * retryCount);
                        continue;
                    }
                    
                    _logger?.LogError(ex, "KMB ETA request timed out after {attempts} attempts", MaxRetryAttempts);
                    return new List<TransportEta>();
                }
                catch (Exception ex)
                {
                    if (retryCount < MaxRetryAttempts)
                    {
                        retryCount++;
                        _logger?.LogWarning(ex, "Error fetching ETA data for KMB stop (attempt {retryCount}/{maxRetries})", 
                            retryCount, MaxRetryAttempts);
                        
                        await Task.Delay(RetryDelayMilliseconds * retryCount);
                        continue;
                    }
                    
                    _logger?.LogError(ex, "Error fetching ETA data for KMB stop {stopId} route {routeNumber} after {attempts} attempts", 
                        stopId, routeNumber, MaxRetryAttempts);
                    return new List<TransportEta>();
                }
            }
        }

        /// <summary>
        /// Fetches ETAs for multiple stops in parallel to improve performance
        /// </summary>
        public async Task<Dictionary<string, List<TransportEta>>> FetchEtasInParallel(List<(string stopId, string routeNumber, string serviceType)> requests, int maxConcurrency = 3)
        {
            if (requests == null || requests.Count == 0)
            {
                return new Dictionary<string, List<TransportEta>>();
            }
            
            var result = new ConcurrentDictionary<string, List<TransportEta>>();
            var semaphore = new SemaphoreSlim(maxConcurrency); // Limit concurrent requests
            var tasks = new List<Task>();
            
            foreach (var request in requests)
            {
                tasks.Add(Task.Run(async () => 
                {
                    try
                    {
                        await semaphore.WaitAsync();
                        var stopEtas = await FetchEtaForStop(request.stopId, request.routeNumber, request.serviceType);
                        
                        if (stopEtas.Count > 0)
                        {
                            result[request.stopId] = stopEtas;
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error fetching ETAs for stop {stopId}, route {routeNumber}", request.stopId, request.routeNumber);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }
            
            await Task.WhenAll(tasks);
            semaphore.Dispose();
            
            return new Dictionary<string, List<TransportEta>>(result);
        }

        /// <summary>
        /// Fetches all ETAs for a KMB route at once (faster method)
        /// Uses memory-efficient processing and cancellation tokens
        /// </summary>
        public async Task<Dictionary<string, List<TransportEta>>> FetchKmbEtaForRoute(string routeId, string routeNumber, string serviceType)
        {
            // Create a cache key
            string cacheKey = $"eta_kmb_route_{routeNumber}_{serviceType}";
            
            // Try to get from cache first using the enhanced cache service
            if (_cacheService.TryGetValue(cacheKey, out Dictionary<string, List<TransportEta>>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved ETAs for KMB route {routeNumber} from cache", routeNumber);
                return cachedEtas;
            }
            
            var result = new Dictionary<string, List<TransportEta>>();
            int retryCount = 0;
            
            // Get all stops for this route from the database for matching when stop IDs are missing
            var routeStops = _databaseService.GetSortedStopsForRoute(routeId);
            
            // Use a ValueDictionary for better performance with value types
            var stopSequenceMap = new Dictionary<int, string>(routeStops?.Count ?? 0);
            
            // Create a mapping of stop sequence to stop ID
            if (routeStops != null && routeStops.Count > 0)
            {
                foreach (var stop in routeStops)
                {
                    if (!string.IsNullOrEmpty(stop.Id) && stop.Sequence > 0)
                    {
                        stopSequenceMap[stop.Sequence] = stop.Id;
                    }
                }
                
                _logger?.LogDebug("Created stopSequenceMap with {count} entries for route {routeNumber}", 
                    stopSequenceMap.Count, routeNumber);
            }
            
            while (true)
            {
                try
                {
                    string url = $"{_kmbRouteEtaBaseUrl}/{routeNumber}/{serviceType}";
                    _logger?.LogDebug("Fetching all KMB route ETAs from: {url}", url);

                    // Use cancellation token to avoid hanging requests
                    using var cts = new CancellationTokenSource();
                    cts.CancelAfter(TimeSpan.FromSeconds(15)); // 15 second timeout for route data (more stops)
                    
                    var response = await _httpClient.GetAsync(url, cts.Token);
                    
                    // Check for specific status codes
                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        _logger?.LogWarning("KMB route ETA data not found: {routeNumber}/{serviceType}", routeNumber, serviceType);
                        return result;
                    }
                    
                    if (!response.IsSuccessStatusCode)
                    {
                        if (retryCount < MaxRetryAttempts)
                        {
                            retryCount++;
                            _logger?.LogWarning("Failed to fetch KMB route ETA data (attempt {retryCount}/{maxRetries}): {statusCode}", 
                                retryCount, MaxRetryAttempts, response.StatusCode);
                            
                            await Task.Delay(RetryDelayMilliseconds * retryCount);
                            continue;
                        }
                        
                        _logger?.LogError("Failed to fetch KMB route ETA data after {attempts} attempts: {statusCode}", 
                            MaxRetryAttempts, response.StatusCode);
                        return result;
                    }
                    
                    // Use ReadAsStreamAsync for better performance with large responses
                    var stream = await response.Content.ReadAsStreamAsync();
                    
                    try
                    {
                        // Try to deserialize the route ETA response
                        var routeEtaResponse = await JsonSerializer.DeserializeAsync<KmbRouteEtaResponse>(stream, _jsonOptions);
                        
                        if (routeEtaResponse?.Data == null || routeEtaResponse.Data.Count == 0)
                        {
                            _logger?.LogWarning("No ETA data found for KMB route {routeNumber} service type {serviceType}", 
                                routeNumber, serviceType);
                            return result;
                        }
                        
                        _logger?.LogInformation("Received {count} ETAs for KMB route {routeNumber} service type {serviceType}", 
                            routeEtaResponse.Data.Count, routeNumber, serviceType);
                        
                        // Process the ETAs by stop sequence more efficiently
                        var etasByStop = routeEtaResponse.Data
                            .GroupBy(eta => eta.Seq)
                            .ToDictionary(g => g.Key, g => g.ToList());
                        
                        // Cache the current time for efficiency
                        DateTime now = DateTime.Now;
                        
                        // Process each group of ETAs for each stop sequence
                        foreach (var stopGroup in etasByStop)
                        {
                            int seq = stopGroup.Key;
                            var etaDataList = stopGroup.Value;
                            
                            if (etaDataList.Count == 0) continue;
                            
                            // Get the first ETA data to extract the stop ID
                            string stopId = etaDataList[0].StopId ?? string.Empty;
                            
                            // If stop ID is null, try to get it from the sequence map
                            if (string.IsNullOrEmpty(stopId) && stopSequenceMap.TryGetValue(seq, out var mappedStopId))
                            {
                                _logger?.LogDebug("Using mapped stop ID {mappedStopId} for sequence {seq} on route {routeNumber}", 
                                    mappedStopId, seq, routeNumber);
                                stopId = mappedStopId;
                            }
                            
                            // Skip if the stop ID is still null after trying to map it
                            if (string.IsNullOrEmpty(stopId))
                            {
                                _logger?.LogWarning("Skipping ETA data with null or empty stop ID for KMB route {routeNumber} sequence {seq}", 
                                    routeNumber, seq);
                                continue;
                            }
                            
                            // Pre-size the list for performance
                            var stopEtas = new List<TransportEta>(etaDataList.Count);
                            
                            foreach (var etaData in etaDataList)
                            {
                                if (string.IsNullOrEmpty(etaData.Eta)) continue;
                                
                                // Parse the ETA time
                                if (!DateTime.TryParseExact(etaData.Eta, "yyyy-MM-ddTHH:mm:ss+08:00", 
                                        CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime etaTime))
                                {
                                    _logger?.LogWarning("Failed to parse ETA time: {etaTime}", etaData.Eta);
                                    continue;
                                }
                                
                                // Use the resolved 'stopId' for constructing etaId to ensure uniqueness
                                string etaId = $"{etaData.Route}_{stopId}_{etaData.EtaSeq}_{etaData.Dir}_{etaTime:yyyyMMddHHmmss}";
                                
                                // Calculate minutes remaining
                                TimeSpan timeRemaining = etaTime - now;
                                int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                                
                                var eta = new TransportEta
                                {
                                    Id = etaId,
                                    StopId = stopId,
                                    RouteId = $"KMB_{etaData.Route}_{serviceType}_{etaData.Dir}", // Restore the correct parameters
                                    RouteNumber = etaData.Route ?? string.Empty,
                                    Direction = etaData.Dir ?? string.Empty, // Fix null warning
                                    ServiceType = serviceType,
                                    FetchTime = now,
                                    Remarks = etaData.Remarks ?? string.Empty, // Fix null warning
                                    EtaTime = etaTime,
                                    RemainingMinutes = minutes <= 0 ? "0" : minutes.ToString(),
                                    IsCancelled = !string.IsNullOrEmpty(etaData.Remarks) && 
                                                etaData.Remarks?.Contains("cancel", StringComparison.OrdinalIgnoreCase) == true // Add null-conditional operator
                                };
                                
                                stopEtas.Add(eta);
                            }
                            
                            // Only add to the result if we have valid ETAs
                            if (stopEtas.Count > 0)
                            {
                                // Additional null check before using stopId as dictionary key
                                if (!string.IsNullOrEmpty(stopId))
                                {
                                    result[stopId] = stopEtas;
                                }
                                else
                                {
                                    _logger?.LogWarning("Skipping adding ETAs with null or empty stop ID for KMB route {routeNumber}", routeNumber);
                                }
                            }
                        }
                        
                        // Cache the result using the enhanced cache service
                        _cacheService.Set(cacheKey, result, EtaCacheExpiration);
                        
                        _logger?.LogDebug("Successfully processed and cached ETAs for {count} stops on route {routeNumber}", 
                            result.Count, routeNumber);
                        return result;
                    }
                    catch (JsonException ex)
                    {
                        if (retryCount < MaxRetryAttempts)
                        {
                            retryCount++;
                            _logger?.LogWarning(ex, "Error deserializing KMB route ETA response (attempt {retryCount}/{maxRetries})", 
                                retryCount, MaxRetryAttempts);
                            
                            await Task.Delay(RetryDelayMilliseconds * retryCount);
                            continue;
                        }
                        
                        _logger?.LogError(ex, "Error deserializing KMB route ETA response after {attempts} attempts", 
                            MaxRetryAttempts);
                        return result;
                    }
                }
                catch (TaskCanceledException ex)
                {
                    if (retryCount < MaxRetryAttempts)
                    {
                        retryCount++;
                        _logger?.LogWarning(ex, "KMB route ETA request timed out (attempt {retryCount}/{maxRetries})", 
                            retryCount, MaxRetryAttempts);
                        
                        await Task.Delay(RetryDelayMilliseconds * retryCount);
                        continue;
                    }
                    
                    _logger?.LogError(ex, "KMB route ETA request timed out after {attempts} attempts", 
                        MaxRetryAttempts);
                    return result;
                }
                catch (Exception ex)
                {
                    if (retryCount < MaxRetryAttempts)
                    {
                        retryCount++;
                        _logger?.LogWarning(ex, "Error fetching KMB route ETA data (attempt {retryCount}/{maxRetries})", 
                            retryCount, MaxRetryAttempts);
                        
                        await Task.Delay(RetryDelayMilliseconds * retryCount);
                        continue;
                    }
                    
                    _logger?.LogError(ex, "Error fetching KMB route ETA data for route {routeNumber} after {attempts} attempts", 
                        routeNumber, MaxRetryAttempts);
                    return result;
                }
            }
        }

        /// <summary>
        /// Generic method to fetch ETA data for a stop
        /// </summary>
        public async Task<List<TransportEta>> FetchEtaForStop(string stopId, string routeNumber, string serviceType)
        {
            // Currently only KMB is supported, others will be added in the future
            return await FetchKmbEtaForStop(stopId, routeNumber, serviceType);
        }

        /// <summary>
        /// Generic method to fetch ETA data for a route
        /// </summary>
        public async Task<Dictionary<string, List<TransportEta>>> FetchEtaForRoute(string routeId, string routeNumber, string serviceType, List<TransportStop> stops)
        {
            // Currently only KMB is supported, others will be added in the future
            return await FetchKmbEtaForRoute(routeId, routeNumber, serviceType);
        }
        
        /// <summary>
        /// Clears the ETA cache for the specified stop
        /// </summary>
        public void ClearEtaCache(string stopId, string routeNumber, string serviceType)
        {
            string cacheKey = $"eta_kmb_{stopId}_{routeNumber}_{serviceType}";
            _cacheService.Remove(cacheKey);
            _logger?.LogDebug("Cleared ETA cache for stop {stopId} route {routeNumber}", stopId, routeNumber);
        }
        
        /// <summary>
        /// Clears the ETA cache for the specified route
        /// </summary>
        public void ClearRouteEtaCache(string routeNumber, string serviceType)
        {
            // Clear both route-level and direction-specific caches
            string routeCacheKey = $"eta_kmb_route_{routeNumber}_{serviceType}";
            _cacheService.Remove(routeCacheKey);
            
            // Also clear inbound and outbound direction caches to ensure fresh data after direction change
            _cacheService.RemoveByPattern($"eta_kmb_route_{routeNumber}_{serviceType}_");
            
            _logger?.LogDebug("Cleared ETA cache for route {routeNumber} with service type {serviceType}", 
                routeNumber, serviceType);
        }
        
        /// <summary>
        /// Clears all ETA caches
        /// </summary>
        public void ClearAllEtaCaches()
        {
            _cacheService.RemoveByPattern("eta_");
            _logger?.LogInformation("Cleared all ETA caches");
        }

        /// <summary>
        /// Converts the KMB ETA API response to our internal TransportEta model
        /// </summary>
        private List<TransportEta> ConvertKmbEtas(KmbEtaResponse etaResponse, string? queryStopId = null)
        {
            if (etaResponse?.Data == null || etaResponse.Data.Count == 0)
            {
                return new List<TransportEta>();
            }
            
            var result = new List<TransportEta>(etaResponse.Data.Count);
            var now = DateTime.Now;
            
            // Count validation issues for logging
            int invalidEtaCount = 0;
            int invalidStopIdCount = 0;
            int invalidTimeFormatCount = 0;
            
            foreach (var etaData in etaResponse.Data)
            {
                // Skip invalid entries
                if (string.IsNullOrEmpty(etaData.Route))
                {
                    invalidEtaCount++;
                    continue;
                }
                
                // If the ETA data has no stop ID but we were given one in the query, use that
                if (string.IsNullOrEmpty(etaData.StopId) && !string.IsNullOrEmpty(queryStopId))
                {
                    etaData.StopId = queryStopId;
                }
                
                // Skip if we still don't have a valid stop ID
                if (string.IsNullOrEmpty(etaData.StopId))
                {
                    invalidStopIdCount++;
                    continue;
                }
                
                // Skip if we don't have a valid ETA time
                if (string.IsNullOrEmpty(etaData.Eta))
                {
                    continue;
                }
                
                // Parse the ETA time
                if (!DateTime.TryParseExact(etaData.Eta, "yyyy-MM-ddTHH:mm:ss+08:00", 
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime etaTime))
                {
                    invalidTimeFormatCount++;
                    continue;
                }
                
                // Restore the correct parameters
                string etaId = $"{etaData.Route}_{etaData.StopId}_{etaData.EtaSeq}_{etaData.Dir}_{etaTime:yyyyMMddHHmmss}";
                
                // Calculate minutes remaining
                TimeSpan timeRemaining = etaTime - now;
                int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                
                var eta = new TransportEta
                {
                    Id = etaId,
                    StopId = etaData.StopId ?? string.Empty, // Ensure StopId is never null
                    RouteId = $"KMB_{etaData.Route}_{etaData.ServiceType.ToString()}_{etaData.Dir}", // Use etaData.ServiceType
                    RouteNumber = etaData.Route ?? string.Empty, // Fix null warning
                    Direction = etaData.Dir ?? string.Empty, // Fix null warning
                    ServiceType = etaData.ServiceType.ToString(), // Use etaData.ServiceType
                    FetchTime = now,
                    Remarks = etaData.Remarks ?? string.Empty, // Ensure Remarks is never null
                    EtaTime = etaTime,
                    RemainingMinutes = minutes <= 0 ? "0" : minutes.ToString(),
                    IsCancelled = !string.IsNullOrEmpty(etaData.Remarks) && 
                                  etaData.Remarks?.Contains("cancel", StringComparison.OrdinalIgnoreCase) == true
                };

                result.Add(eta);
            }

            // Log validation issues if any
            if (invalidEtaCount > 0 || invalidStopIdCount > 0 || invalidTimeFormatCount > 0)
            {
                _logger?.LogWarning("Validation issues while processing ETAs: " +
                                    "Invalid ETA: {invalidEta}, Invalid StopID: {invalidStopId}, " +
                                    "Invalid Time Format: {invalidTimeFormat}",
                    invalidEtaCount, invalidStopIdCount, invalidTimeFormatCount);
            }
            
            return result;
        }

        /// <summary>
        /// Fetches all ETAs for a specific KMB stop without filtering for a specific route
        /// </summary>
        public async Task<List<TransportEta>> FetchAllKmbEtaForStop(string stopId)
        {
            string cacheKey = $"eta_all_kmb_{stopId}";
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved all ETAs for KMB stop {stopId} from cache", stopId);
                return cachedEtas;
            }
            
            // If we recently determined this stop has no ETAs, return empty list quickly
            if (_stopHasNoEtas.ContainsKey(stopId))
                {
                _logger?.LogDebug("Stop {stopId} previously determined to have no ETAs, returning empty list from quick check.", stopId);
                    return new List<TransportEta>();
                }
                
                int retryCount = 0;
            string url = string.Empty; // Declare url outside the try block
                while (true)
                {
                    try
                    {
                    // await ApplyRateLimiting(); // Rate limiting handled by HttpClientUtility

                    // Modify StopId for KMB API if needed
                    string kmbApiStopId = stopId.StartsWith("KMB_") ? stopId.Substring(4) : stopId;
                    url = $"{_kmbStopSpecificEtaBaseUrl}/{kmbApiStopId}"; // Assign value here
                    _logger?.LogDebug("Fetching all KMB ETAs for stop from: {url} (original stopId: {originalStopId})", url, stopId);

                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)); // 10-second timeout
                    var response = await _httpClient.GetAsync(url, cts.Token);
                        
                        if (!response.IsSuccessStatusCode)
                        {
                        // Log the initial failure
                        _logger?.LogWarning("[EtaService] KMB ETA API call for stop {kmbStopId} failed. Status: {statusCode}, URL: {apiUrl}",
                            stopId, response.StatusCode, url);
                                
                        if (response.StatusCode == HttpStatusCode.UnprocessableEntity)
                        {
                            // For 422, typically means bad request (e.g. invalid stopId).
                            // Attempt one immediate retry, just in case of a very transient issue.
                            if (retryCount < 1) // Max 1 retry for 422 (total 2 attempts)
                            {
                                retryCount++;
                                _logger?.LogInformation("[EtaService] UnprocessableEntity (422) for stop {kmbStopId}. Attempting 1 quick retry ({retryAttempt}/1). URL: {apiUrl}",
                                    stopId, retryCount, url);
                                // Optional: await Task.Delay(200); // Very short delay if desired
                                continue;
                            }
                            _logger?.LogWarning("[EtaService] Persistent UnprocessableEntity (422) for stop {kmbStopId} after {retries} attempts. Giving up. URL: {apiUrl}",
                                stopId, retryCount + 1, url);
                            // Cache empty result effectively happens when we return new List<TransportEta>() 
                            // and the caller (e.g., GetCachedEtas) caches it.
                            return new List<TransportEta>();
                        }
                        else if ((int)response.StatusCode >= 500 && (int)response.StatusCode < 600) // Server-side errors (5xx)
                        {
                            const int maxServerRetries = 2;
                            if (retryCount < maxServerRetries) 
                            {
                                retryCount++;
                                _logger?.LogInformation("[EtaService] Server error {statusCode} for stop {kmbStopId}. Retrying (Attempt {retryAttempt}/{maxRetries}). URL: {apiUrl}",
                                    response.StatusCode, stopId, retryCount, maxServerRetries, url);
                                await Task.Delay(retryCount * RetryDelayMilliseconds); // Standard delay
                                continue;
                            }
                            _logger?.LogError("[EtaService] Persistent server error {statusCode} for stop {kmbStopId} after {retries} attempts. Giving up. URL: {apiUrl}",
                                response.StatusCode, stopId, retryCount + 1, url);
                            return new List<TransportEta>();
                        }
                        else // Other client errors (4xx, excluding 422 handled above) or unexpected status codes
                        {
                            _logger?.LogWarning("[EtaService] Client error {statusCode} (non-422/5xx) for stop {kmbStopId}. Not retrying. URL: {apiUrl}",
                                response.StatusCode, stopId, url);
                            return new List<TransportEta>();
                        }
                    }
                    
                    // If successful, reset retryCount (though typically we return shortly after success)
                    // retryCount = 0; 
                        
                        // Read the response as JSON
                        var responseData = await response.Content.ReadAsStreamAsync();
                        var kmbResponse = await JsonSerializer.DeserializeAsync<KmbEtaResponse>(responseData, _jsonOptions);
                        
                        if (kmbResponse?.Data == null)
                        {
                        _logger?.LogWarning("KMB ETA API returned null response or data for stop {kmbStopId} (URL: {apiUrl})", stopId, url);
                        _stopHasNoEtas[stopId] = true;
                        var emptyList = new List<TransportEta>();
                        return emptyList;
                        }
                        
                        _logger?.LogDebug("Processing ETAs for KMB stop {kmbStopId} with auto-assignment of stop ID for ETAs with empty stop ID", stopId);
                        
                    // Convert to internal format
                    var result = ConvertKmbEtas(kmbResponse, stopId); // Pass only kmbResponse and stopId
                    
                    // Cache the results - use a shorter expiration for stops with no ETAs
                    TimeSpan cacheExpiry = result.Count > 0 ? TimeSpan.FromSeconds(25) : TimeSpan.FromSeconds(15);
                    _cacheService.Set(cacheKey, result, cacheExpiry);
                        
                    // Record if this stop has ETAs
                    _stopHasNoEtas[stopId] = result.Count == 0;
                        
                    // Complete the task
                        return result;
                    }
                    catch (HttpRequestException ex)
                    {
                    // Handle HTTP request errors
                    _logger?.LogError(ex, "HTTP request error fetching ETAs for KMB stop {kmbStopId}: {message} (URL: {apiUrl})", 
                        stopId, ex.Message, url);
                        return new List<TransportEta>();
                    }
                    catch (JsonException ex)
                    {
                    // Handle JSON deserialization errors
                    _logger?.LogError(ex, "JSON deserialization error fetching ETAs for KMB stop {kmbStopId}: {message} (URL: {apiUrl})", 
                        stopId, ex.Message, url);
                    if (retryCount < 2) // Allow retry for transient parsing issues if any
                    {
                        retryCount++;
                        await Task.Delay(RetryDelayMilliseconds * retryCount); // Exponential backoff
                        continue; 
                    }
                        return new List<TransportEta>();
                    }
                    catch (TaskCanceledException ex)
                    {
                    // Handle task cancellation (timeout)
                    _logger?.LogWarning(ex, "Task canceled (timeout) fetching ETAs for KMB stop {kmbStopId} (URL: {apiUrl})", 
                        stopId, url);
                    if (retryCount < 2) // Allow retry for timeout
                    {
                        retryCount++;
                        await Task.Delay(RetryDelayMilliseconds * retryCount); // Exponential backoff
                        continue;
                    }
                        return new List<TransportEta>();
                    }
                    catch (Exception ex)
                    {
                        // Handle all other errors
                    _logger?.LogError(ex, "Unexpected error fetching ETAs for KMB stop {kmbStopId}: {message} (URL: {apiUrl})", 
                        stopId, ex.Message, url);
                        return new List<TransportEta>();
                }
            }
        }

        // Response classes remain the same
        private class KmbEtaResponse
        {
            [JsonPropertyName("type")]
            public string? Type { get; set; }
            
            [JsonPropertyName("version")]
            public string? Version { get; set; }
            
            [JsonPropertyName("generated_timestamp")]
            public string? GeneratedTimestamp { get; set; }
            
            [JsonPropertyName("data")]
            public List<KmbEtaData>? Data { get; set; }
        }
        
        private class KmbRouteEtaResponse
        {
            [JsonPropertyName("type")]
            public string? Type { get; set; }
            
            [JsonPropertyName("version")]
            public string? Version { get; set; }
            
            [JsonPropertyName("generated_timestamp")]
            public string? GeneratedTimestamp { get; set; }
            
            [JsonPropertyName("data")]
            public List<KmbEtaData>? Data { get; set; }
        }
        
        private class KmbEtaData
        {
            [JsonPropertyName("co")]
            public string? Company { get; set; }
            
            [JsonPropertyName("route")]
            public string? Route { get; set; }
            
            [JsonPropertyName("dir")]
            public string? Dir { get; set; }
            
            [JsonPropertyName("service_type")]
            public int ServiceType { get; set; }
            
            [JsonPropertyName("stop_id")]
            public string? StopId { get; set; }
            
            [JsonPropertyName("seq")]
            public int Seq { get; set; }
            
            [JsonPropertyName("dest_tc")]
            public string? DestTc { get; set; }
            
            [JsonPropertyName("dest_sc")]
            public string? DestSc { get; set; }
            
            [JsonPropertyName("dest_en")]
            public string? DestEn { get; set; }
            
            [JsonPropertyName("eta")]
            public string? Eta { get; set; }
            
            [JsonPropertyName("eta_seq")]
            public int EtaSeq { get; set; }
            
            [JsonPropertyName("rmk_tc")]
            public string? RmkTc { get; set; }
            
            [JsonPropertyName("rmk_sc")]
            public string? RmkSc { get; set; }
            
            [JsonPropertyName("rmk_en")]
            public string? RmkEn { get; set; }
            
            [JsonPropertyName("dest")]
            public string? Dest { get; set; }
            
            [JsonPropertyName("remarks")]
            public string? Remarks { get; set; }
            
            [JsonPropertyName("data_timestamp")]
            public string? DataTimestamp { get; set; }
        }

        /// <summary>
        /// Checks if the EtaService has fresh ETAs in cache for a specific stop
        /// </summary>
        public bool HasCachedEtas(string stopId)
        {
            if (string.IsNullOrEmpty(stopId))
                return false;
        
            string cacheKey = $"eta_kmb_all_{stopId}";
        
            // Check if we have a cache entry for all ETAs for this stop
            return _cacheService.TryGetValue(cacheKey, out List<TransportEta>? _);
        }

        /// <summary>
        /// Gets cached ETAs for a specific stop from the cache if available,
        /// otherwise fetches new data from the API
        /// </summary>
        public async Task<List<TransportEta>> GetCachedEtas(string stopId)
        {
            if (string.IsNullOrEmpty(stopId))
                return new List<TransportEta>();
        
            string cacheKey = $"eta_kmb_all_{stopId}";
        
            // Try to get from cache first
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved cached ETAs for KMB stop {stopId}", stopId);
                return cachedEtas;
            }
        
            // If not in cache, fetch new data
            return await FetchAllKmbEtaForStop(stopId);
        }

        /// <summary>
        /// Gets ETAs for a specific route and set of stops from the database
        /// </summary>
        /// <param name="routeId">The route ID</param>
        /// <param name="stopIds">List of stop IDs to fetch ETAs for</param>
        /// <returns>Dictionary of stop IDs to list of ETAs</returns>
        public async Task<Dictionary<string, List<TransportEta>>> GetEtasForRouteFromDb(string routeId, List<string> stopIds)
        {
            var result = new Dictionary<string, List<TransportEta>>();
            
            try
            {
                if (string.IsNullOrEmpty(routeId) || stopIds == null || stopIds.Count == 0)
                {
                    return result;
                }
                
                // Only get ETAs from the last 5 minutes to ensure they're still relevant
                var cutoffTime = DateTime.UtcNow.AddMinutes(-5);
                
                // Get all ETAs for this route that match the stop IDs
                var allEtas = _databaseService.GetAllRecords<TransportEta>("TransportEtas")
                    .Where(eta => 
                        eta.RouteId == routeId && 
                        stopIds.Contains(eta.StopId) && 
                        eta.EtaTime > cutoffTime)
                    .ToList();
                
                // Group ETAs by stop ID
                foreach (var eta in allEtas)
                {
                    if (!result.ContainsKey(eta.StopId))
                    {
                        result[eta.StopId] = new List<TransportEta>();
                    }
                    
                    result[eta.StopId].Add(eta);
                }
                
                // Log what we found
                _logger?.LogDebug("Found {count} ETAs for route {routeId} in database", 
                    allEtas.Count, routeId);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error getting ETAs from database for route {routeId}", routeId);
            }
            
            return result;
        }

        // Queue an ETA fetch request and get a Task that will complete when the data is available
        public Task<List<TransportEta>> QueueEtaFetchRequest(string stopId, string? routeNumber = null, string? serviceType = null, int priority = 5)
        {
            // Check if we already have this in cache first
            string cacheKey = routeNumber != null && serviceType != null 
                ? $"eta_kmb_{stopId}_{routeNumber}_{serviceType}"
                : $"eta_kmb_all_{stopId}";
            
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("QueueEtaFetchRequest: Using cached ETAs for stop {stopId}", stopId);
                return Task.FromResult(cachedEtas);
            }
            
            var request = new EtaFetchRequest
            {
                StopId = stopId,
                RouteNumber = routeNumber,
                ServiceType = serviceType,
                Priority = priority
            };
            
            _etaFetchQueue.Enqueue(request);
            
            // Start the processing task if it's not already running
            Task.Run(() => EnsureQueueProcessing());
            
            return request.CompletionSource.Task;
        }

        // Ensure the queue processing task is running
        private async Task EnsureQueueProcessing()
        {
            if (_isProcessingQueue)
                return;
        
            // Use semaphore to ensure only one thread tries to start processing
            await _processingFlag.WaitAsync();
        
            try
            {
                if (_isProcessingQueue)
                    return;
            
                _isProcessingQueue = true;
            
                // Start processing task
                _ = Task.Run(ProcessEtaQueue);
            }
            finally
            {
                _processingFlag.Release();
            }
        }

        // Process the ETA queue
        private async Task ProcessEtaQueue()
        {
            try
            {
                while (_etaFetchQueue.TryDequeue(out var request))
                {
                    try
                    {
                        List<TransportEta> result;
                        
                        // Process this request
                        if (request.RouteNumber != null && request.ServiceType != null)
                        {
                            // Specific route ETA
                            result = await FetchKmbEtaForStop(request.StopId, request.RouteNumber, request.ServiceType);
                        }
                        else
                        {
                            // All ETAs for a stop
                            result = await FetchAllKmbEtaForStop(request.StopId);
                        }
                        
                        // Complete the task with the result
                        request.CompletionSource.SetResult(result);
                        
                        // Apply rate limiting between requests
                        await ApplyRateLimiting();
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error processing ETA request for stop {stopId}", request.StopId);
                        request.CompletionSource.SetException(ex);
                    }
                }
            }
            finally
            {
                // Mark processing as complete
                _isProcessingQueue = false;
            
                // Check if new items were added to the queue while we were processing
                if (!_etaFetchQueue.IsEmpty)
                {
                    await Task.Delay(100); // Small delay to avoid CPU spinning
                    _ = Task.Run(ProcessEtaQueue);
                }
            }
        }

        // Add this method to optimize ETA fetching with batching and better caching
        public async Task<List<TransportEta>> FetchAllKmbEtaForStopOptimized(string stopId, string? queryStopId = null)
        {
            try
            {
                // First check cache to avoid unnecessary API calls
                string cacheKey = $"eta_kmb_all_{stopId}";
                if (_cacheService.TryGetValue(cacheKey, out List<TransportEta> cachedEtas))
                {
                    _logger?.LogDebug("Using cached ETAs for stop {stopId}", stopId);
                    return cachedEtas;
                }

                // Check if we already have a pending request for this stop
                if (_pendingEtaRequests.TryGetValue(stopId, out var pendingTask))
                {
                    _logger?.LogDebug("Reusing pending ETA request for stop {stopId}", stopId);
                    return await pendingTask;
                }

                // Create a new task completion source for this request
                var tcs = new TaskCompletionSource<List<TransportEta>>();
                _pendingEtaRequests[stopId] = tcs.Task;

                try
                {
                    // Apply rate limiting based on last request time
                    await ApplyRateLimiting();
                    
                    // Skip delays for stops we've never fetched or that had ETAs previously
                    bool skipDelay = !_stopHasNoEtas.ContainsKey(stopId) || !_stopHasNoEtas[stopId];
                    
                    if (!skipDelay)
                    {
                        _logger?.LogDebug("Skipping API rate limiting for stop {stopId} as previous request had no ETAs", stopId);
                    }
                    
                    // Record the time of this request
                    _lastRequestTime = DateTime.Now;

                    // Make the API request using existing httpClient
                    string kmbApiStopId = stopId.StartsWith("KMB_") ? stopId.Substring(4) : stopId;
                    string url = $"{_kmbStopSpecificEtaBaseUrl}/{kmbApiStopId}";
                    
                    HttpResponseMessage response = await _httpClient.GetAsync(url);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        // Read the response as JSON
                        var responseData = await response.Content.ReadAsStreamAsync();
                        var kmbResponse = await JsonSerializer.DeserializeAsync<KmbEtaResponse>(responseData, _jsonOptions);
                        
                        if (kmbResponse?.Data == null)
                        {
                            _logger?.LogWarning("KMB ETA API returned null response or data for stop {kmbStopId}", stopId);
                            _stopHasNoEtas[stopId] = true;
                            var emptyList = new List<TransportEta>();
                            tcs.SetResult(emptyList);
                            _cacheService.Set(cacheKey, emptyList, TimeSpan.FromSeconds(15));
                            _pendingEtaRequests.TryRemove(stopId, out _);
                            return emptyList;
                        }
                        
                        _logger?.LogDebug("Processing ETAs for KMB stop {kmbStopId} with auto-assignment of stop ID for ETAs with empty stop ID", stopId);
                        
                        // Convert to internal format
                        var result = ConvertKmbEtas(kmbResponse, stopId); // Pass only kmbResponse and stopId
                        
                        // Cache the results - use a shorter expiration for stops with no ETAs
                        TimeSpan cacheExpiry = result.Count > 0 ? TimeSpan.FromSeconds(25) : TimeSpan.FromSeconds(15);
                        _cacheService.Set(cacheKey, result, cacheExpiry);
                        
                        // Record if this stop has ETAs
                        _stopHasNoEtas[stopId] = result.Count == 0;
                        
                        // Complete the task
                        tcs.SetResult(result);
                        _pendingEtaRequests.TryRemove(stopId, out _);
                        
                        _logger?.LogDebug("Successfully retrieved {count} ETAs for KMB stop {kmbStopId}", result.Count, stopId);
                        return result;
                    }
                    else
                    {
                        _logger?.LogWarning("KMB ETA API returned error status code for stop {kmbStopId}: {status}", 
                            stopId, response.StatusCode);
                        var emptyList = new List<TransportEta>();
                        tcs.SetResult(emptyList);
                        _pendingEtaRequests.TryRemove(stopId, out _);
                        return emptyList;
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error fetching ETAs from KMB API for stop {kmbStopId}", stopId);
                    tcs.SetException(ex);
                    _pendingEtaRequests.TryRemove(stopId, out _);
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in FetchAllKmbEtaForStopOptimized for stop {kmbStopId}", stopId);
                return new List<TransportEta>();
            }
        }
    }
} 