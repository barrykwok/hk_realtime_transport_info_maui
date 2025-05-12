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
        private readonly DatabaseService _databaseService;
        private readonly ILogger<EtaService> _logger;
        private readonly string _kmbStopEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/eta";
        private readonly string _kmbRouteEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta";
        
        // Cached JsonSerializerOptions to avoid recreating them on each request
        private readonly JsonSerializerOptions _jsonOptions;
        
        // Enhanced cache service
        private readonly CacheService _cacheService;
        
        // Constants for retry logic
        private const int MaxRetryAttempts = 3;
        private const int RetryDelayMilliseconds = 1000;
        
        // Cache expiration times
        private static readonly TimeSpan EtaCacheExpiration = TimeSpan.FromSeconds(25); // ETAs refresh every 30 seconds

        // Add a semaphore to limit concurrent API requests - reduce from 3 to 2 max concurrent requests
        private readonly SemaphoreSlim _apiRequestSemaphore = new SemaphoreSlim(2, 2);
        
        // Add additional rate limiting
        private readonly object _lastRequestLock = new object();
        private DateTime _lastRequestTime = DateTime.MinValue;
        private const int MinRequestIntervalMs = 300; // Minimum time between API requests

        public EtaService(HttpClient httpClient, DatabaseService databaseService, ILogger<EtaService> logger, CacheService cacheService)
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
            int delayMs = 0;
            
            lock (_lastRequestLock)
            {
                // Calculate time since last request
                TimeSpan timeSinceLastRequest = now - _lastRequestTime;
                
                // If we need to delay, calculate how long
                if (timeSinceLastRequest.TotalMilliseconds < MinRequestIntervalMs)
                {
                    delayMs = (int)(MinRequestIntervalMs - timeSinceLastRequest.TotalMilliseconds);
                }
                
                // Update last request time (use projected time after delay)
                _lastRequestTime = now.AddMilliseconds(delayMs);
            }
            
            // Apply delay if needed
            if (delayMs > 0)
            {
                _logger?.LogDebug("API rate limiting: delaying request by {delayMs}ms", delayMs);
                await Task.Delay(delayMs);
            }
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
                            
                            // Generate a deterministic ID based on route, stop, and ETA time
                            string etaId = $"{routeNumber}_{stopId}_{etaData.EtaSeq}_{etaTime:yyyyMMddHHmmss}";

                            // Calculate minutes remaining
                            TimeSpan timeRemaining = etaTime - now;
                            int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                            
                            var eta = new TransportEta
                            {
                                Id = etaId,
                                StopId = etaData.StopId ?? string.Empty, // Ensure StopId is never null
                                RouteNumber = etaData.Route ?? string.Empty, // Fix null warning
                                Direction = etaData.Dir ?? string.Empty, // Fix null warning
                                ServiceType = etaData.ServiceType.ToString(), // Convert int to string
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
                                
                                // Generate a deterministic ID based on route, stop, and ETA time
                                string etaId = $"{routeNumber}_{stopId}_{etaData.EtaSeq}_{etaTime:yyyyMMddHHmmss}";
                                
                                // Calculate minutes remaining
                                TimeSpan timeRemaining = etaTime - now;
                                int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                                
                                var eta = new TransportEta
                                {
                                    Id = etaId,
                                    StopId = stopId,
                                    RouteNumber = routeNumber,
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
                
                // Generate a deterministic ID based on route, stop, and ETA time
                string etaId = $"{etaData.Route}_{etaData.StopId}_{etaData.EtaSeq}_{etaTime:yyyyMMddHHmmss}";
                
                // Calculate minutes remaining
                TimeSpan timeRemaining = etaTime - now;
                int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                
                var eta = new TransportEta
                {
                    Id = etaId,
                    StopId = etaData.StopId ?? string.Empty, // Ensure StopId is never null
                    RouteNumber = etaData.Route ?? string.Empty, // Fix null warning
                    Direction = etaData.Dir ?? string.Empty, // Fix null warning
                    ServiceType = etaData.ServiceType.ToString(), // Convert int to string
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
            if (string.IsNullOrEmpty(stopId))
            {
                _logger?.LogWarning("Attempted to fetch ETAs for null or empty stop ID");
                return new List<TransportEta>();
            }
            
            // Create a cache key
            string cacheKey = $"eta_kmb_all_{stopId}";
            
            // Try to get from cache first using the enhanced cache service
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved all ETAs for KMB stop {kmbStopId} from cache ({count} ETAs)", 
                    stopId, cachedEtas.Count);
                return cachedEtas;
            }
            
            _logger?.LogDebug("Cache miss for ETAs at KMB stop ID {kmbStopId}, fetching from API", stopId);
            
            // Use semaphore to limit concurrent API calls
            bool semaphoreAcquired = false;
            try
            {
                // Apply general rate limiting first
                await ApplyRateLimiting();
                
                // Then try to get a slot for the API call
                semaphoreAcquired = await _apiRequestSemaphore.WaitAsync(TimeSpan.FromSeconds(15));
                
                if (!semaphoreAcquired)
                {
                    _logger?.LogWarning("Could not acquire semaphore to make API call for KMB stop {kmbStopId} after waiting 15 seconds", stopId);
                    return new List<TransportEta>();
                }
                
                // Construct the URL for the KMB ETA API
                string url = $"https://data.etabus.gov.hk/v1/transport/kmb/stop-eta/{stopId}";
                
                int retryCount = 0;
                while (true)
                {
                    try
                    {
                        // Make the API request using HttpClient directly
                        using var response = await _httpClient.GetAsync(url);
                        
                        if (!response.IsSuccessStatusCode)
                        {
                            _logger?.LogWarning("KMB ETA API returned status code {statusCode} for stop {kmbStopId}", 
                                response.StatusCode, stopId);
                                
                            if (retryCount < 2 && (int)response.StatusCode >= 500)
                            {
                                retryCount++;
                                // Wait before retrying (exponential backoff)
                                int delayMs = retryCount * 1000;
                                await Task.Delay(delayMs);
                                continue;
                            }
                            
                            return new List<TransportEta>();
                        }
                        
                        // Read the response as JSON
                        var responseData = await response.Content.ReadAsStreamAsync();
                        var kmbResponse = await JsonSerializer.DeserializeAsync<KmbEtaResponse>(responseData, _jsonOptions);
                        
                        if (kmbResponse?.Data == null)
                        {
                            _logger?.LogWarning("KMB ETA API returned null response or data for stop {kmbStopId}", stopId);
                            return new List<TransportEta>();
                        }
                        
                        _logger?.LogDebug("Processing ETAs for KMB stop {kmbStopId} with auto-assignment of stop ID for ETAs with empty stop ID", stopId);
                        
                        // Convert the API response to our TransportEta model
                        // Pass the stopId as queryStopId parameter to handle ETAs with empty stop IDs
                        var result = ConvertKmbEtas(kmbResponse, stopId);
                        
                        // Log success
                        _logger?.LogDebug("Successfully retrieved {count} ETAs for KMB stop {kmbStopId}", 
                            result.Count, stopId);
                        
                        // Cache the result using our enhanced cache service
                        _cacheService.Set(cacheKey, result, EtaCacheExpiration);
                        
                        return result;
                    }
                    catch (HttpRequestException ex)
                    {
                        // Handle HTTP errors (like network issues, server errors)
                        retryCount++;
                        
                        // Log the error
                        _logger?.LogWarning(ex, "HTTP error fetching ETAs for KMB stop {kmbStopId}, attempt {attempt}: {message}", 
                            stopId, retryCount, ex.Message);
                        
                        // Implement exponential backoff
                        if (retryCount <= 2)
                        {
                            // Wait before retrying (exponential backoff)
                            int delayMs = retryCount * 1000;
                            await Task.Delay(delayMs);
                            continue;
                        }
                        
                        // After max retries, return empty list
                        _logger?.LogError(ex, "Max retries reached for KMB stop {kmbStopId}, giving up", stopId);
                        return new List<TransportEta>();
                    }
                    catch (JsonException ex)
                    {
                        // Handle JSON parsing errors
                        _logger?.LogError(ex, "JSON error parsing KMB ETA response for stop {kmbStopId}: {message}", 
                            stopId, ex.Message);
                        return new List<TransportEta>();
                    }
                    catch (TaskCanceledException ex)
                    {
                        // Handle timeouts
                        _logger?.LogWarning(ex, "Request timed out fetching ETAs for KMB stop {kmbStopId}", stopId);
                        return new List<TransportEta>();
                    }
                    catch (Exception ex)
                    {
                        // Handle all other errors
                        _logger?.LogError(ex, "Unexpected error fetching ETAs for KMB stop {kmbStopId}: {message}", 
                            stopId, ex.Message);
                        return new List<TransportEta>();
                    }
                }
            }
            finally
            {
                // Always release the semaphore if we acquired it
                if (semaphoreAcquired)
                {
                    _apiRequestSemaphore.Release();
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
    }
} 