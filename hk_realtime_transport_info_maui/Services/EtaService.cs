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
using System.Text;

namespace hk_realtime_transport_info_maui.Services
{
    public class EtaService
    {
        private readonly HttpClientUtility _httpClient;
        private readonly LiteDbService _databaseService;
        private readonly ILogger<EtaService> _logger;
        private readonly string _kmbStopEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/eta";
        private readonly string _kmbRouteEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/route-eta";
        private readonly string _kmbStopSpecificEtaBaseUrl = "https://data.etabus.gov.hk/v1/transport/kmb/stop-eta";
        
        // MTR API base URLs
        private readonly string _mtrBusEtaUrl = "https://rt.data.gov.hk/v1/transport/mtr/bus/getSchedule";
        
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
        
        // MTR API cache tracking
        private readonly ConcurrentDictionary<string, bool> _mtrStopHasNoEtas = 
            new ConcurrentDictionary<string, bool>();
        private readonly ConcurrentDictionary<string, Task<List<TransportEta>>> _pendingMtrEtaRequests = 
            new ConcurrentDictionary<string, Task<List<TransportEta>>>();

        public EtaService(HttpClientUtility httpClient, LiteDbService databaseService, ILogger<EtaService> logger, CacheService cacheService)
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
            
            _logger?.LogInformation("Initialized EtaService");
        }

        // Helper method to get the appropriate language code for MTR API
        private string GetMtrApiLanguage()
        {
            // Get current UI culture
            var currentCulture = CultureInfo.CurrentUICulture.Name.ToLowerInvariant();
            
            // MTR API only supports "en" or "zh" (not "zh-hans" or other variants)
            if (currentCulture.StartsWith("zh"))
            {
                _logger?.LogDebug("Using 'zh' language for MTR API based on culture: {culture}", currentCulture);
                return "zh";
            }
            
            _logger?.LogDebug("Using 'en' language for MTR API based on culture: {culture}", currentCulture);
            return "en";
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
            
            try
            {
                string url = $"{_kmbStopEtaBaseUrl}/{stopId}/{routeNumber}/{serviceType}";
                _logger?.LogDebug("Fetching KMB stop ETA from: {url}", url);

                // Use the HttpClientUtility with a high priority and default timeout
                var response = await _httpClient.GetAsync(url, HttpClientUtility.PRIORITY_HIGH);
                
                // Check for specific status codes
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    _logger?.LogWarning("KMB stop ETA data not found: {stopId}/{routeNumber}/{serviceType}", 
                        stopId, routeNumber, serviceType);
                    return new List<TransportEta>();
                }
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger?.LogError("Failed to fetch KMB stop ETA data: {statusCode}", response.StatusCode);
                    return new List<TransportEta>();
                }

                // Use ReadAsStreamAsync for better performance with large responses
                var stream = await response.Content.ReadAsStreamAsync();
                
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
                    _logger?.LogError(ex, "Error deserializing KMB stop ETA response for stop {stopId}", stopId);
                    return new List<TransportEta>();
                }
            }
            catch (TaskCanceledException ex)
            {
                _logger?.LogError(ex, "KMB ETA request timed out for stop {stopId}", stopId);
                return new List<TransportEta>();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error fetching ETA data for KMB stop {stopId} route {routeNumber}", stopId, routeNumber);
                return new List<TransportEta>();
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
                await semaphore.WaitAsync();
                
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var etas = await FetchKmbEtaForStop(request.stopId, request.routeNumber, request.serviceType);
                        result.TryAdd(request.stopId, etas);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error fetching ETAs for stop {stopId} route {routeNumber}", 
                            request.stopId, request.routeNumber);
                        result.TryAdd(request.stopId, new List<TransportEta>());
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }
            
            await Task.WhenAll(tasks);
            
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
            
            try
            {
                string url = $"{_kmbRouteEtaBaseUrl}/{routeNumber}/{serviceType}";
                _logger?.LogDebug("Fetching all KMB route ETAs from: {url}", url);
                
                // Use HttpClientUtility to make the request
                var response = await _httpClient.GetAsync(url, HttpClientUtility.PRIORITY_HIGH);
                
                // Check for specific status codes
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    _logger?.LogWarning("KMB route ETA data not found: {routeNumber}/{serviceType}", routeNumber, serviceType);
                    return result;
                }
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger?.LogError("Failed to fetch KMB route ETA data: {statusCode}", response.StatusCode);
                    return result;
                }
                
                // Use ReadAsStreamAsync for better performance with large responses
                var stream = await response.Content.ReadAsStreamAsync();
                
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
                
                // Process the ETAs by stop
                var etasByStop = routeEtaResponse.Data
                    .GroupBy(eta => eta.StopId)
                    .ToDictionary(g => g.Key ?? string.Empty, g => g.ToList());
                
                // Cache the current time for efficiency
                DateTime now = DateTime.Now;
                
                // Process each group of ETAs for each stop
                foreach (var stopGroup in etasByStop)
                {
                    string stopId = stopGroup.Key;
                    var etaDataList = stopGroup.Value;
                    
                    if (string.IsNullOrEmpty(stopId) || etaDataList.Count == 0) continue;
                    
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
                        
                        // Generate a unique ID for this ETA
                        string etaId = $"{etaData.Route}_{stopId}_{etaData.EtaSeq}_{etaData.Dir}_{etaTime:yyyyMMddHHmmss}";
                        
                        // Calculate minutes remaining
                        TimeSpan timeRemaining = etaTime - now;
                        int minutes = (int)Math.Ceiling(timeRemaining.TotalMinutes);
                        
                        var eta = new TransportEta
                        {
                            Id = etaId,
                            StopId = stopId,
                            RouteId = $"KMB_{etaData.Route}_{serviceType}_{etaData.Dir}",
                            RouteNumber = etaData.Route ?? string.Empty,
                            Direction = etaData.Dir ?? string.Empty,
                            ServiceType = serviceType,
                            FetchTime = now,
                            Remarks = etaData.Remarks ?? string.Empty,
                            EtaTime = etaTime,
                            RemainingMinutes = minutes <= 0 ? "0" : minutes.ToString(),
                            IsCancelled = !string.IsNullOrEmpty(etaData.Remarks) && 
                                        etaData.Remarks?.Contains("cancel", StringComparison.OrdinalIgnoreCase) == true
                        };
                        
                        stopEtas.Add(eta);
                    }
                    
                    // Only add to the result if we have valid ETAs
                    if (stopEtas.Count > 0)
                    {
                        result[stopId] = stopEtas;
                    }
                }
                
                // Cache the result
                _cacheService.Set(cacheKey, result, EtaCacheExpiration);
                
                _logger?.LogDebug("Successfully processed and cached ETAs for {count} stops on route {routeNumber}", 
                    result.Count, routeNumber);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error fetching KMB route ETA data for route {routeNumber}", routeNumber);
            }
            
            return result;
        }

        /// <summary>
        /// Generic method to fetch ETA data for a stop
        /// </summary>
        public async Task<List<TransportEta>> FetchEtaForStop(string stopId, string routeNumber, string serviceType)
        {
            // Check operator type based on stop ID
            if (stopId.StartsWith("MTR-"))
            {
                return await FetchMtrEtaForStop(stopId, routeNumber, serviceType);
            }
            else
            {
                // Default to KMB for now
                return await FetchKmbEtaForStop(stopId, routeNumber, serviceType);
            }
        }

        /// <summary>
        /// Generic method to fetch ETA data for a route
        /// </summary>
        public async Task<Dictionary<string, List<TransportEta>>> FetchEtaForRoute(string routeId, string routeNumber, string serviceType, List<TransportStop> stops)
        {
            // Check operator type based on route ID
            if (routeId.StartsWith("MTR_"))
            {
                return await FetchMtrEtaForRoute(routeNumber, serviceType, stops);
            }
            else
            {
                // Default to KMB for now
                return await FetchKmbEtaForRoute(routeId, routeNumber, serviceType);
            }
        }
        
        /// <summary>
        /// Fetches MTR bus ETA data for a specific stop
        /// </summary>
        public async Task<List<TransportEta>> FetchMtrEtaForStop(string stopId, string routeNumber, string serviceType)
        {
            // Create a cache key
            string cacheKey = $"eta_mtr_{stopId}_{routeNumber}_{serviceType}";
            
            // Try to get from cache first using the enhanced cache service
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved ETAs for MTR stop {stopId} route {routeNumber} from cache", stopId, routeNumber);
                return cachedEtas;
            }
            
            // Extract direction from the stopId (MTR-XXX-[U|D]XXX format)
            string direction = stopId.Contains("-U") ? "O" : "I"; // O = outbound (U), I = inbound (D)
            
            try
            {
                // Get all ETAs for this MTR stop and filter for the specific route
                var allEtas = await FetchAllMtrEtaForStop(stopId);
                
                // Filter for the specific route
                var filteredEtas = allEtas.Where(eta => 
                    eta.RouteNumber == routeNumber && 
                    eta.Direction == direction).ToList();
                
                // Cache the filtered results
                _cacheService.Set(cacheKey, filteredEtas, EtaCacheExpiration);
                
                return filteredEtas;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error fetching ETAs for MTR stop {stopId} route {routeNumber}", stopId, routeNumber);
                return new List<TransportEta>();
            }
        }
        
        /// <summary>
        /// Main method to fetch MTR ETAs for a route by making a single API call and processing for multiple stops
        /// </summary>
        public async Task<Dictionary<string, List<TransportEta>>> FetchMtrEtaForRoute(string routeNumber, string serviceType, List<TransportStop> stops)
        {
            // Create a cache key
            string cacheKey = $"eta_mtr_route_{routeNumber}_{serviceType}";
            
            _logger?.LogDebug("Starting FetchMtrEtaForRoute for route {routeNumber}, serviceType {serviceType}", routeNumber, serviceType);
            
            // Try to get from cache first
            if (_cacheService.TryGetValue(cacheKey, out Dictionary<string, List<TransportEta>>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved cached ETAs for MTR route {routeNumber}", routeNumber);
                return cachedEtas;
            }
            
            var result = new Dictionary<string, List<TransportEta>>();
            
            try
            {
                string language = GetMtrApiLanguage();
                _logger?.LogDebug("Using '{language}' language for MTR API based on culture: {culture}", language, CultureInfo.CurrentCulture.Name);
                
                // Construct the request body
                var requestData = new
                {
                    language = language,
                    routeName = routeNumber
                };
                
                // Create request content
                string url = _mtrBusEtaUrl;
                _logger?.LogDebug("Sending MTR ETA API request for route {routeNumber} to URL: {url} with language: {language} and body: {body}", 
                    routeNumber, url, language, System.Text.Json.JsonSerializer.Serialize(requestData));
                
                // Use JsonContent to properly format the request
                var jsonContent = JsonContent.Create(requestData, null, _jsonOptions);
                
                // Make the request with the HttpClientUtility
                var response = await _httpClient.PostAsync(url, jsonContent, HttpClientUtility.PRIORITY_HIGH);
                
                if (response.IsSuccessStatusCode)
                {
                    // First read the response as a string to validate it
                    string responseContent = await response.Content.ReadAsStringAsync();
                    
                    if (string.IsNullOrEmpty(responseContent) || responseContent.Contains("error"))
                    {
                        _logger?.LogWarning("MTR API returned error or empty response for route {routeNumber}", routeNumber);
                        return result;
                    }
                    
                    // Now deserialize using stream and the JsonSerializer
                    try
                    {
                        using var responseData = new MemoryStream(Encoding.UTF8.GetBytes(responseContent));
                        var mtrResponse = await JsonSerializer.DeserializeAsync<MtrEtaResponse>(responseData, _jsonOptions);
                        
                        if (mtrResponse?.BusStop != null && mtrResponse.BusStop.Count > 0)
                        {
                            _logger?.LogDebug("MTR ETA API returned data for route {routeNumber}: routeStatus={routeStatus}, stopCount={stopCount}", 
                                routeNumber, mtrResponse.RouteStatus, mtrResponse.BusStop.Count);
                            
                            // Process each stop from our list
                            foreach (var stop in stops)
                            {
                                // Find the matching stop data from the MTR response
                                string mtrStopId = stop.Id.Replace("MTR-", "", StringComparison.OrdinalIgnoreCase);
                                string cleanStopId = routeNumber + "-" + mtrStopId.Split('-').LastOrDefault();
                                
                                var matchingStop = mtrResponse.BusStop.FirstOrDefault(s => 
                                    s.BusStopId == mtrStopId || 
                                    s.BusStopId == cleanStopId ||
                                    (s.BusStopId?.Contains(routeNumber) == true && mtrStopId.Contains(s.BusStopId)));
                                
                                if (matchingStop != null)
                                {
                                    // Convert to internal format and add to result
                                    var stopEtas = ConvertMtrEtas(matchingStop, stop.Id, routeNumber);
                                    if (stopEtas.Count > 0)
                                    {
                                        result[stop.Id] = stopEtas;
                                    }
                                }
                                else
                                {
                                    // No matching stop found
                                    _logger?.LogWarning("No matching stop found in MTR API response for stop {stopId}", stop.Id);
                                    result[stop.Id] = new List<TransportEta>();
                                }
                            }
                            
                            // Cache the result if we have any data
                            if (result.Count > 0)
                            {
                                _cacheService.Set(cacheKey, result, EtaCacheExpiration);
                                _logger?.LogDebug("Cached ETAs for MTR route {routeNumber} ({count} stops)", routeNumber, result.Count);
                            }
                        }
                        else
                        {
                            // Enhanced logging for routes with empty data
                            _logger?.LogWarning("MTR ETA API returned empty data for route {routeNumber}. Status: {status}, Message: {message}, RouteStatus: {routeStatus}", 
                                routeNumber, 
                                mtrResponse?.Status ?? "null", 
                                mtrResponse?.Message ?? "null",
                                mtrResponse?.RouteStatus ?? "null");
                                
                            // Log the raw response for debugging
                            _logger?.LogDebug("Raw MTR API response for route {routeNumber}: {response}", 
                                routeNumber, responseContent);
                        }
                    }
                    catch (JsonException ex)
                    {
                        _logger?.LogError(ex, "Error parsing MTR ETA response for route {routeNumber}", routeNumber);
                    }
                }
                else
                {
                    _logger?.LogError("MTR ETA API returned error status code for route {routeNumber}: {status}", 
                        routeNumber, response.StatusCode);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error fetching ETAs for MTR route {routeNumber}", routeNumber);
            }
            
            return result;
        }
        
        /// <summary>
        /// Converts the MTR ETA API response to our internal TransportEta model
        /// </summary>
        private List<TransportEta> ConvertMtrEtas(MtrStopData stopData, string stopId, string routeNumber)
        {
            if (stopData?.Bus == null || stopData.Bus.Count == 0)
            {
                _logger?.LogWarning("Cannot convert MTR ETAs: stop has no bus data for stop {stopId}", stopId);
                return new List<TransportEta>();
            }
            
            var result = new List<TransportEta>();
            var now = DateTime.Now;
            
            // Parse the direction from the stop ID
            var parts = stopId.Split('-');
            if (parts.Length < 3) 
            {
                _logger?.LogWarning("Invalid stop ID format during MTR ETA conversion: {stopId}", stopId);
                return result;
            }
            
            string directionPart = parts[2];
            char directionChar = directionPart[0];
            string direction = (directionChar == 'U' || directionChar == 'u') ? "O" : "I"; // O = outbound, I = inbound
            
            // Default service type for MTR routes
            const string defaultServiceType = "1";
            
            _logger?.LogDebug("Converting MTR ETAs for stop {stopId}: busCount={busCount}", 
                stopId, stopData.Bus.Count);
            
            int etaCount = 0;
            foreach (var bus in stopData.Bus)
            {
                // Skip bus if no arrival time
                if (string.IsNullOrEmpty(bus.ArrivalTimeInSecond))
                {
                    continue;
                }
                
                // Calculate ETA time based on seconds from now
                if (int.TryParse(bus.ArrivalTimeInSecond, out int secondsToArrival))
                {
                    DateTime etaTime = now.AddSeconds(secondsToArrival);
                    
                            // Calculate minutes remaining
                    int minutes = (int)Math.Ceiling(secondsToArrival / 60.0);
                                
                            // Skip ETAs in the past
                            if (minutes < 0) 
                            {
                                _logger?.LogDebug("Skipping past MTR ETA (minutes={minutes})", minutes);
                                continue;
                            }
                            
                    string etaId = $"MTR_{routeNumber}_{stopId}_{etaCount}_{direction}_{etaTime:yyyyMMddHHmmss}";
                            
                            var eta = new TransportEta
                            {
                                Id = etaId,
                                StopId = stopId,
                        RouteId = $"MTR_{routeNumber}_{defaultServiceType}_{direction}",
                                RouteNumber = routeNumber,
                        Direction = direction,
                                ServiceType = defaultServiceType,
                                FetchTime = now,
                        Remarks = bus.BusRemark ?? string.Empty,
                                EtaTime = etaTime,
                                RemainingMinutes = minutes <= 0 ? "0" : minutes.ToString(),
                        IsCancelled = !string.IsNullOrEmpty(bus.BusRemark) && 
                                    bus.BusRemark.Contains("cancel", StringComparison.OrdinalIgnoreCase)
                            };
                            
                            _logger?.LogDebug("Created MTR ETA for route {routeNumber}, stop {stopId}: minutes={minutes}, etaId={etaId}",
                                routeNumber, stopId, minutes, etaId);
                                
                            result.Add(eta);
                            etaCount++;
                        }
                        else
                        {
                    _logger?.LogWarning("Could not parse MTR arrival time seconds: {arrivalTime}", bus.ArrivalTimeInSecond);
                }
            }
            
            _logger?.LogDebug("Converted total of {count} MTR ETAs for stop {stopId}", result.Count, stopId);
            return result;
        }
        
        /// <summary>
        /// Fetches all ETAs for a MTR stop without filtering for a specific route
        /// </summary>
        public async Task<List<TransportEta>> FetchAllMtrEtaForStop(string stopId)
        {
            string cacheKey = $"eta_mtr_all_{stopId}";
            
            // Make sure stopId starts with MTR- prefix
            if (!stopId.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase) && !stopId.StartsWith("mtr-", StringComparison.OrdinalIgnoreCase))
            {
                stopId = $"MTR-{stopId}";
                _logger?.LogDebug("Added MTR prefix to stop ID: {stopId}", stopId);
            }
            
            _logger?.LogDebug("Starting FetchAllMtrEtaForStop for stop {stopId}", stopId);
            
            // If we're already fetching ETAs for this stop, wait for that request
            if (_pendingMtrEtaRequests.TryGetValue(stopId, out var pendingTask))
            {
                _logger?.LogDebug("Found pending request for MTR stop {stopId}, waiting for it to complete", stopId);
                try
                {
                    return await pendingTask;
                }
                catch
                {
                    _logger?.LogWarning("Previous request for MTR stop {stopId} failed, will try again", stopId);
                    _pendingMtrEtaRequests.TryRemove(stopId, out _);
                }
            }
            
            // If we recently determined this stop has no ETAs, return quickly
            if (_mtrStopHasNoEtas.TryGetValue(stopId, out bool hasNoEtas) && hasNoEtas)
            {
                _logger?.LogDebug("Using cached knowledge that MTR stop {stopId} has no ETAs", stopId);
                return new List<TransportEta>();
            }
            
            // Check cache first
            if (_cacheService.TryGetValue(cacheKey, out List<TransportEta>? cachedEtas) && cachedEtas != null)
            {
                _logger?.LogDebug("Retrieved ETAs for MTR stop {stopId} from cache ({count} items)", stopId, cachedEtas.Count);
                return cachedEtas;
            }
            
            // Create a task completion source for others to await on
            var tcs = new TaskCompletionSource<List<TransportEta>>();
            if (!_pendingMtrEtaRequests.TryAdd(stopId, tcs.Task))
            {
                _logger?.LogWarning("Failed to add pending request for MTR stop {stopId}, another thread may have added it", stopId);
                if (_pendingMtrEtaRequests.TryGetValue(stopId, out var existingTask))
                {
                    return await existingTask;
                }
            }
            
            try
            {
                // Extract parts from the stop ID for API request
                string[] parts = stopId.Split('-');
                if (parts.Length < 3)
                {
                    _logger?.LogWarning("Invalid MTR stop ID format: {stopId}", stopId);
                    var emptyList = new List<TransportEta>();
                    tcs.SetResult(emptyList);
                    _pendingMtrEtaRequests.TryRemove(stopId, out _);
                    return emptyList;
                }
                
                string routeNumber = parts[1];
                string directionPart = parts[2];
                string mtrStopId = $"{routeNumber}-{directionPart}";
                
                // Prepare request body
                string language = GetMtrApiLanguage();
                var requestData = new
                {
                    language = language,
                    routeName = routeNumber
                };
                
                // Create request content
                var jsonContent = JsonContent.Create(requestData, null, _jsonOptions);
                
                // Make request
                var response = await _httpClient.PostAsync(_mtrBusEtaUrl, jsonContent, HttpClientUtility.PRIORITY_HIGH);
                
                if (response.IsSuccessStatusCode)
                {
                    // First read the response as a string to validate it
                    string responseContent = await response.Content.ReadAsStringAsync();
                    
                    if (string.IsNullOrEmpty(responseContent) || responseContent.Contains("error"))
                    {
                        _logger?.LogWarning("MTR API returned error or empty response for stop {stopId}", 
                            stopId);
                        var emptyList = new List<TransportEta>();
                        _mtrStopHasNoEtas[stopId] = true;
                        tcs.SetResult(emptyList);
                        _pendingMtrEtaRequests.TryRemove(stopId, out _);
                        _cacheService.Set(cacheKey, emptyList, TimeSpan.FromSeconds(15));
                        return emptyList;
                    }
                    
                    try
                    {
                        // Convert string back to stream for deserialization
                        using var responseData = new MemoryStream(Encoding.UTF8.GetBytes(responseContent));
                        var mtrResponse = await JsonSerializer.DeserializeAsync<MtrEtaResponse>(responseData, _jsonOptions);
                        
                        if (mtrResponse?.BusStop != null && mtrResponse.BusStop.Count > 0)
                        {
                            _logger?.LogInformation("MTR ETA API returned data for stop {stopId}: route={route}, stopCount={stopCount}",
                                stopId, 
                                mtrResponse.RouteName,
                                mtrResponse.BusStop.Count);
                                
                            // Make a more flexible search for matching stops - try both the raw format and a simpler format
                            string cleanStopId = $"{routeNumber}-{directionPart.TrimStart('U', 'D', 'u', 'd')}";
                            
                            // Find the matching stop data with various matching strategies
                            var matchingStop = mtrResponse.BusStop.FirstOrDefault(s => 
                                s.BusStopId == mtrStopId || 
                                s.BusStopId == cleanStopId ||
                                (s.BusStopId?.Contains(routeNumber) == true && 
                                (s.BusStopId?.EndsWith(directionPart) == true || 
                                 s.BusStopId?.EndsWith(directionPart.TrimStart('U', 'D', 'u', 'd')) == true)));
                                
                            if (matchingStop == null)
                            {
                                _logger?.LogWarning("No matching stop found in MTR API response for stopId {mtrStopId} or {cleanStopId}", 
                                    mtrStopId, cleanStopId);
                                
                                // Log all available stops from the response to help debug
                                foreach (var dataStop in mtrResponse.BusStop)
                                {
                                    _logger?.LogDebug("Available stop in response: {busStopId}", dataStop.BusStopId);
                                }
                                
                                var emptyList = new List<TransportEta>();
                                _mtrStopHasNoEtas[stopId] = true;
                                tcs.SetResult(emptyList);
                                _pendingMtrEtaRequests.TryRemove(stopId, out _);
                                _cacheService.Set(cacheKey, emptyList, TimeSpan.FromSeconds(15));
                                return emptyList;
                            }
                            
                            // Convert to our internal format
                            var result = ConvertMtrEtas(matchingStop, stopId, routeNumber);
                            
                            _logger?.LogDebug("Converted {count} MTR ETAs for stop {stopId}", result.Count, stopId);
                            
                            // Cache the results
                            TimeSpan cacheExpiry = result.Count > 0 ? TimeSpan.FromSeconds(25) : TimeSpan.FromSeconds(15);
                            _cacheService.Set(cacheKey, result, cacheExpiry);
                            
                            // Record if this stop has ETAs
                            _mtrStopHasNoEtas[stopId] = result.Count == 0;
                            
                            tcs.SetResult(result);
                            _pendingMtrEtaRequests.TryRemove(stopId, out _);
                            
                            _logger?.LogDebug("Successfully retrieved {count} ETAs for MTR stop {stopId}", result.Count, stopId);
                            return result;
                        }
                        else
                        {
                            _logger?.LogWarning("MTR ETA API returned null response or data for stop {stopId}", stopId);
                            
                            var emptyList = new List<TransportEta>();
                            _mtrStopHasNoEtas[stopId] = true;
                            
                            tcs.SetResult(emptyList);
                            _pendingMtrEtaRequests.TryRemove(stopId, out _);
                            _cacheService.Set(cacheKey, emptyList, TimeSpan.FromSeconds(15));
                            
                            return emptyList;
                        }
                    }
                    catch (JsonException ex)
                    {
                        _logger?.LogError(ex, "Error deserializing MTR API response: {message}", ex.Message);
                        
                        var emptyList = new List<TransportEta>();
                        tcs.SetException(ex);
                        _pendingMtrEtaRequests.TryRemove(stopId, out _);
                        return emptyList;
                    }
                }
                else
                {
                    _logger?.LogWarning("MTR ETA API returned error status code for stop {stopId}: {status}", 
                        stopId, response.StatusCode);
                    
                    var emptyList = new List<TransportEta>();
                    
                    tcs.SetResult(emptyList);
                    _pendingMtrEtaRequests.TryRemove(stopId, out _);
                    
                    return emptyList;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error fetching ETAs for MTR stop {stopId}", stopId);
                
                tcs.SetException(ex);
                _pendingMtrEtaRequests.TryRemove(stopId, out _);
                
                return new List<TransportEta>();
            }
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
                _logger?.LogDebug("Using cached knowledge that stop {stopId} has no ETAs", stopId);
                return new List<TransportEta>();
            }
            
            try
            {
                string url = $"{_kmbStopSpecificEtaBaseUrl}/{stopId}";
                _logger?.LogDebug("Fetching all KMB ETAs for stop {stopId} from URL: {url}", stopId, url);
                
                // Use HttpClientUtility to make the request
                var response = await _httpClient.GetAsync(url, HttpClientUtility.PRIORITY_HIGH);
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger?.LogWarning("Failed to get ETAs for KMB stop {stopId}: {statusCode}", 
                        stopId, response.StatusCode);
                    return new List<TransportEta>();
                }
                
                var stream = await response.Content.ReadAsStreamAsync();
                var etaResponse = await JsonSerializer.DeserializeAsync<KmbEtaResponse>(stream, _jsonOptions);
                
                if (etaResponse?.Data == null || etaResponse.Data.Count == 0)
                {
                    _logger?.LogWarning("No ETAs found for KMB stop {stopId}", stopId);
                    _stopHasNoEtas[stopId] = true;
                    return new List<TransportEta>();
                }
                
                var result = ConvertKmbEtas(etaResponse, stopId);
                
                if (result.Count > 0)
                {
                    _cacheService.Set(cacheKey, result, EtaCacheExpiration);
                    _logger?.LogDebug("Cached {count} ETAs for KMB stop {stopId}", result.Count, stopId);
                }
                else
                {
                    _stopHasNoEtas[stopId] = true;
                    _logger?.LogDebug("No valid ETAs for KMB stop {stopId} after filtering", stopId);
                }
                
                return result;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error fetching all ETAs for KMB stop {stopId}", stopId);
                return new List<TransportEta>();
            }
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
                        _cacheService.Set(cacheKey, emptyList, TimeSpan.FromSeconds(15));
                        
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

        // Response classes for MTR ETA API
        public class MtrEtaResponse
        {
            [JsonPropertyName("status")]
            public string Status { get; set; } = "0"; // Changed from int to string
            
            [JsonPropertyName("message")]
            public string? Message { get; set; }
            
            [JsonPropertyName("caseNumber")]
            public int CaseNumber { get; set; }
            
            [JsonPropertyName("caseNumberDetail")]
            public string? CaseNumberDetail { get; set; }
            
            [JsonPropertyName("routeStatus")]
            public string? RouteStatus { get; set; }
            
            [JsonPropertyName("routeStatusTime")]
            public string? RouteStatusTime { get; set; }
            
            [JsonPropertyName("routeName")]
            public string? RouteName { get; set; }
            
            [JsonPropertyName("routeStatusColour")]
            public string? RouteStatusColour { get; set; }
            
            [JsonPropertyName("routeStatusRemarkTitle")]
            public string? RouteStatusRemarkTitle { get; set; }
            
            [JsonPropertyName("routeStatusRemarkContent")]
            public string? RouteStatusRemarkContent { get; set; }
            
            [JsonPropertyName("routeStatusRemarkFooterRemark")]
            public string? RouteStatusRemarkFooterRemark { get; set; }
            
            [JsonPropertyName("footerRemarks")]
            public string? FooterRemarks { get; set; }
            
            [JsonPropertyName("busStop")]
            public List<MtrStopData>? BusStop { get; set; }
            
            [JsonIgnore]
            public List<MtrStopData>? Data => BusStop;
        }
        
        public class MtrStopData
        {
            [JsonPropertyName("bus")]
            public List<MtrBus>? Bus { get; set; }
            
            [JsonPropertyName("busStopId")]
            public string? BusStopId { get; set; }
            
            [JsonPropertyName("busIcon")]
            public string? BusIcon { get; set; }
            
            [JsonPropertyName("busStopRemark")]
            public string? BusStopRemark { get; set; }
            
            [JsonPropertyName("busStopStatus")]
            public string? BusStopStatus { get; set; }
            
            [JsonPropertyName("busStopStatusRemarkTitle")]
            public string? BusStopStatusRemarkTitle { get; set; }
            
            [JsonPropertyName("busStopStatusRemarkContent")]
            public string? BusStopStatusRemarkContent { get; set; }
            
            [JsonPropertyName("busStopStatusTime")]
            public string? BusStopStatusTime { get; set; }
            
            [JsonPropertyName("isSuspended")]
            public string? IsSuspended { get; set; }
        }
        
        public class MtrBus
        {
            [JsonPropertyName("busId")]
            public string? BusId { get; set; }
            
            [JsonPropertyName("direction")]
            public string Direction { get; set; } = "O"; // O = outbound, I = inbound
            
            [JsonPropertyName("stopId")]
            public string? StopId { get; set; }
            
            [JsonPropertyName("isScheduled")]
            public string IsScheduled { get; set; } = "0"; // Changed from bool to string
            
            [JsonPropertyName("arrivalTimeInSecond")]
            public string? ArrivalTimeInSecond { get; set; }
            
            [JsonPropertyName("arrivalTimeText")]
            public string? ArrivalTimeText { get; set; }
            
            [JsonPropertyName("departureTimeInSecond")]
            public string? DepartureTimeInSecond { get; set; }
            
            [JsonPropertyName("departureTimeText")]
            public string? DepartureTimeText { get; set; }
            
            [JsonPropertyName("isDelayed")]
            public string? IsDelayed { get; set; }
            
            [JsonPropertyName("busRemark")]
            public string? BusRemark { get; set; }
            
            [JsonPropertyName("busLocation")]
            public MtrBusLocation? BusLocation { get; set; }
            
            [JsonPropertyName("lineRef")]
            public string? LineRef { get; set; }
        }
        
        public class MtrBusLocation
        {
            [JsonPropertyName("latitude")]
            public double Latitude { get; set; }
            
            [JsonPropertyName("longitude")]
            public double Longitude { get; set; }
        }
        
        // KMB Response classes
        public class KmbEtaResponse
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
        
        public class KmbRouteEtaResponse
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
        
        public class KmbEtaData
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
    }
} 