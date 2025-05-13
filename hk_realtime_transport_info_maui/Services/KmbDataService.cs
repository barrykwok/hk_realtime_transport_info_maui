using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using hk_realtime_transport_info_maui.Models;
using Microsoft.Extensions.Logging;
using NGeoHash;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Maui.Controls;

namespace hk_realtime_transport_info_maui.Services
{
    public class DownloadProgressEventArgs : EventArgs
    {
        public double Progress { get; }
        public string StatusMessage { get; }
        public bool DataWasUpdated { get; }
        
        public DownloadProgressEventArgs(double progress, string statusMessage, bool dataWasUpdated = true)
        {
            Progress = progress;
            StatusMessage = statusMessage;
            DataWasUpdated = dataWasUpdated;
        }
    }
    
    public class KmbDataService
    {
        private readonly LiteDbService _databaseService;
        private readonly HttpClientUtility _httpClient;
        private readonly ILogger<KmbDataService> _logger;
        private readonly JsonSerializerOptions _jsonOptions;
        private const int MAX_RETRY = 3;
        private const int RETRY_DELAY_MS = 1000;
        
        // Property to suppress downloads
        private bool _suppressDownloads;
        public bool SuppressDownloads 
        { 
            get => _suppressDownloads; 
            set 
            {
                if (_suppressDownloads != value)
                {
                    _suppressDownloads = value;
                    _logger?.LogInformation("{0} SuppressDownloads changed to {1}", LOG_PREFIX, value);
                }
            }
        }
        
        // Constants for standardized logging
        private const string LOG_PREFIX = "[KmbDataService]";
        private const string LOG_TRANSPORT_TYPE = "KMB";
        private const string LOG_SEPARATOR = "====================";
        
        // Data freshness settings
        private static readonly TimeSpan DATA_EXPIRATION = TimeSpan.FromDays(7);
        
        // Cleanup settings
        private static readonly TimeSpan DATA_CLEANUP_AGE = TimeSpan.FromDays(1); // Clean data older than 1 day
        
        // Background processing control
        private readonly CancellationTokenSource _downloadCts = new CancellationTokenSource();
        private Task? _currentDownloadTask;
        private bool _isDownloading;
        
        // Timestamp of the current download operation - used for cleanup
        private DateTime _currentDownloadTimestamp;
        
        // Batch sizes - increased for better performance
        private const int STOP_BATCH_SIZE = 500; // Further reduced to avoid memory pressure
        private const int ROUTE_BATCH_SIZE = 250; // Further reduced to avoid memory pressure
        private const int ROUTE_STOP_BATCH_SIZE = 250; // Further reduced to avoid memory pressure
        
        // Progress reporting
        public event EventHandler<DownloadProgressEventArgs>? ProgressChanged;
        
        // Cache the data freshness results to avoid excessive database access
        private static readonly Dictionary<TransportOperator, (bool exists, bool fresh, DateTime timestamp)> _dataFreshnessCache = 
            new Dictionary<TransportOperator, (bool exists, bool fresh, DateTime timestamp)>();
        
        // Cache expiration timespan - only recheck every few minutes
        private static readonly TimeSpan _freshnessCheckExpiration = TimeSpan.FromMinutes(10);
        
        public KmbDataService(LiteDbService databaseService, HttpClientUtility httpClient, ILogger<KmbDataService> logger)
        {
            _databaseService = databaseService;
            _httpClient = httpClient;
            _logger = logger;
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
        }
        
        protected virtual void OnProgressChanged(double progress, string statusMessage, bool dataWasUpdated = true)
        {
            // Always dispatch progress events to the main thread
            MainThread.BeginInvokeOnMainThread(() => {
                ProgressChanged?.Invoke(this, new DownloadProgressEventArgs(progress, statusMessage, dataWasUpdated));
            });
        }
        
        /// <summary>
        /// Checks if data exists for a specific operator and whether it's fresh or expired
        /// </summary>
        /// <param name="operatorType">The transport operator to check</param>
        /// <returns>Tuple with (dataExists, dataIsFresh)</returns>
        private (bool dataExists, bool dataIsFresh) CheckDataFreshness(TransportOperator operatorType)
        {
            // Check cache first
            if (_dataFreshnessCache.TryGetValue(operatorType, out var cachedResult))
            {
                // Use cached result if it's recent
                if ((DateTime.UtcNow - cachedResult.timestamp) < _freshnessCheckExpiration)
                {
                    _logger?.LogInformation("Using cached data freshness result for {operator}: Exists={exists}, Fresh={fresh}",
                        operatorType, cachedResult.exists, cachedResult.fresh);
                    return (cachedResult.exists, cachedResult.fresh);
                }
            }
            
            try
            {
                // Get all records directly from LiteDb since the DatabaseService has been migrated
                var routes = _databaseService.GetAllRecords<TransportRoute>("TransportRoutes")
                    .Where(r => r.Operator == operatorType)
                    .ToList();
                
                bool dataExists = routes.Count > 0;
                
                if (!dataExists)
                {
                    _logger?.LogInformation("No data found for operator {operator}", operatorType);
                    
                    // Store in cache
                    _dataFreshnessCache[operatorType] = (false, false, DateTime.UtcNow);
                    return (false, false);
                }
                
                // Check data freshness (using the newest route's timestamp)
                var newestRoute = routes.OrderByDescending(r => r.LastUpdated).FirstOrDefault();
                if (newestRoute == null)
                {
                    _logger?.LogWarning("No routes found for {operator} despite previous check", operatorType);
                    
                    // Store in cache
                    _dataFreshnessCache[operatorType] = (false, false, DateTime.UtcNow);
                    return (false, false);
                }
                
                var dataAge = DateTime.UtcNow - newestRoute.LastUpdated;
                bool dataIsFresh = dataAge <= DATA_EXPIRATION;
                
                _logger?.LogInformation("Data for {operator}: Age={age:F1} days, Fresh={fresh}", 
                    operatorType, dataAge.TotalDays, dataIsFresh);
                
                // Store result in cache
                _dataFreshnessCache[operatorType] = (dataExists, dataIsFresh, DateTime.UtcNow);
                return (dataExists, dataIsFresh);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error checking data freshness for {operator}", operatorType);
                
                // CRITICAL CHANGE: In case of database error, assume data exists and is fresh
                // This prevents unnecessary downloads when database access fails temporarily
                _logger?.LogWarning("Database error occurred. Assuming data exists and is fresh to prevent unnecessary download");
                
                // Store result in cache with assumption that data exists
                _dataFreshnessCache[operatorType] = (true, true, DateTime.UtcNow);
                return (true, true);
            }
        }

        /// <summary>
        /// Starts the data download process for KMB data
        /// </summary>
        public Task DownloadAllDataAsync(bool forceRefresh = false)
        {
            // Check SuppressDownloads flag first to prevent any downloads
            if (SuppressDownloads)
            {
                _logger?.LogInformation("{0} Download suppressed by SuppressDownloads flag", LOG_PREFIX);
                // Return a completed task to avoid any download operations
                return Task.CompletedTask;
            }
            
            // If already downloading, return the current task
            if (_isDownloading && _currentDownloadTask != null)
            {
                return _currentDownloadTask;
            }
            
            // Use a local variable to track if download is needed
            bool downloadNeeded = forceRefresh;
            
            // CRITICAL IMPROVEMENT: First check - Absolute earliest check for auto-refresh suppression
            if (Microsoft.Maui.Controls.Application.Current is App app && app.SuppressAutoDataRefresh)
            {
                _logger?.LogInformation("Data download suppressed by App.SuppressAutoDataRefresh flag (early check)");
                // Send update indicating successful completion but no actual data refresh
                OnProgressChanged(1.0, "Data refresh suppressed", false);
                OnProgressChanged(1.0, "DOWNLOAD_COMPLETE", false);
                return Task.CompletedTask;
            }
            
            // Skip download if already in progress
            if (_isDownloading)
            {
                _logger.LogWarning("Download already in progress, ignoring new request");
                return Task.CompletedTask;
            }
            
            // Additional check: Check data freshness first unless force refresh is specified
            if (!forceRefresh)
            {
                var (kmbDataExists, kmbDataIsFresh) = CheckDataFreshness(TransportOperator.KMB);
                
                // Cache the result for internal use
                bool cachedDataExists = kmbDataExists;
                bool cachedDataIsFresh = kmbDataIsFresh;
                
                _logger?.LogInformation("Data freshness check: exists={exists}, fresh={fresh}", 
                    cachedDataExists, cachedDataIsFresh);
                
                // Only download if data doesn't exist or isn't fresh
                downloadNeeded = !cachedDataExists || !cachedDataIsFresh;
                
                if (!downloadNeeded)
                {
                    _logger?.LogInformation("Skipping download, data is fresh");
                    // Signal complete with no change
                    OnProgressChanged(1.0, "Data is up to date", false);
                    OnProgressChanged(1.0, "DOWNLOAD_COMPLETE", false);
                    return Task.CompletedTask;
                }
            }
            
            // CRITICAL IMPROVEMENT: Second check - Check again in case flag changed during data freshness check
            if (Microsoft.Maui.Controls.Application.Current is App app2 && app2.SuppressAutoDataRefresh)
            {
                _logger?.LogInformation("Data download suppressed by App.SuppressAutoDataRefresh flag (second check)");
                OnProgressChanged(1.0, "Data refresh suppressed", false);
                OnProgressChanged(1.0, "DOWNLOAD_COMPLETE", false);
                return Task.CompletedTask;
            }
            
            _isDownloading = true;
            
            // Cancel any existing download
            if (_currentDownloadTask != null && !_currentDownloadTask.IsCompleted)
            {
                _downloadCts.Cancel();
                _logger?.LogInformation("Cancelled existing download task");
            }
            
            // Create a new cancellation token source
            _downloadCts.Dispose();
            var cts = new CancellationTokenSource();
            
            // Create a TaskCompletionSource to track the download completion
            var taskCompletionSource = new TaskCompletionSource<bool>();
            
            // Start a new download task
            _currentDownloadTask = Task.Run(async () => 
            {
                try
                {
                    // Double-check one more time if the app wants to suppress download
                    if (Microsoft.Maui.Controls.Application.Current is App checkApp && checkApp.SuppressAutoDataRefresh)
                    {
                        _logger?.LogDebug("Data download suppressed right before starting download");
                        _isDownloading = false;
                        OnProgressChanged(1.0, "Download suppressed", false); 
                        taskCompletionSource.TrySetResult(false);
                        return;
                    }
                    
                    await DownloadAllDataInternalAsync(cts.Token, forceRefresh, downloadNeeded);
                    taskCompletionSource.TrySetResult(true);
                }
                catch (TaskCanceledException)
                {
                    _logger?.LogInformation("Download task was cancelled");
                    taskCompletionSource.TrySetCanceled();
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error in download task");
                    taskCompletionSource.TrySetException(ex);
                }
                finally
                {
                    _isDownloading = false;
                }
            }, cts.Token);
            
            return taskCompletionSource.Task;
        }
        
        /// <summary>
        /// Internal method that performs the actual download work
        /// </summary>
        private async Task DownloadAllDataInternalAsync(CancellationToken cancellationToken, bool forceRefresh = false, bool downloadNeeded = true)
        {
            try
            {
                // CRITICAL IMPROVEMENT: One more check at the start of the actual download work
                if (Microsoft.Maui.Controls.Application.Current is App checkApp && checkApp.SuppressAutoDataRefresh)
                {
                    _logger?.LogInformation("Data download suppressed right before download work (in DownloadAllDataInternalAsync)");
                    _isDownloading = false;
                    OnProgressChanged(1.0, "Download suppressed", false);
                    OnProgressChanged(1.0, "DOWNLOAD_COMPLETE", false);
                    return;
                }
                
                // Set the timestamp for this download operation
                _currentDownloadTimestamp = DateTime.UtcNow;
                _logger?.LogInformation("{Prefix} Started download operation at {Timestamp}", LOG_PREFIX, _currentDownloadTimestamp);
                
                // Skip the freshness check if we already performed it in the calling method
                if (!downloadNeeded && !forceRefresh)
                {
                    // Skip download if data exists and is fresh
                    _logger?.LogInformation("{Prefix} {Separator}", LOG_PREFIX, LOG_SEPARATOR);
                    _logger?.LogInformation("{Prefix} Skipping {Operator} data download, data is fresh (less than 7 days old)", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                    _logger?.LogInformation("{Prefix} {Separator}", LOG_PREFIX, LOG_SEPARATOR);
                    
                    // Use false for DataWasUpdated to signal no data was refreshed
                    OnProgressChanged(1.0, "Data is up to date", false);
                    OnProgressChanged(1.0, "DOWNLOAD_COMPLETE", false);
                    return;
                }
                
                // Starting download process
                _logger?.LogInformation("{Prefix} {Separator}", LOG_PREFIX, LOG_SEPARATOR);
                _logger?.LogInformation("{Prefix} Starting {Operator} data download", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                _logger?.LogInformation("{Prefix} {Separator}", LOG_PREFIX, LOG_SEPARATOR);
                
                OnProgressChanged(0, "Starting download...");
                
                // Throttle CPU usage for background processing
                // This ensures the UI thread has enough resources
                Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                
                // Step 1 & 2: Download stops and routes in parallel (60% of progress) 
                _logger?.LogDebug("Starting parallel download of stops and routes");
                
                OnProgressChanged(0.05, "Downloading data in parallel...");
                
                try
                {
                    // Create tasks for parallel execution
                    var stopsTask = DownloadStopsAsync(cancellationToken);
                    var routesTask = DownloadRoutesAsync(cancellationToken);
                    
                    // Wait for both downloads to complete
                    await Task.WhenAll(stopsTask, routesTask);
                    OnProgressChanged(0.6, "Routes and stops downloaded");
                    
                    // Give the UI thread a chance to process events
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error during parallel data download, continuing with route-stops");
                    OnProgressChanged(0.6, "Partial data downloaded");
                }
                
                // Step 3: Download route-stops and merge data (40% of progress)
                _logger?.LogDebug("Step 3: Downloading route-stops");
                
                OnProgressChanged(0.65, "Downloading route-stops...");
                try
                {
                    await DownloadRouteStopsAsync(cancellationToken);
                    OnProgressChanged(0.95, "Download completed");
                    
                    // Give the UI thread a chance to process events
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to download route-stops");
                    OnProgressChanged(0.95, "Download partially completed");
                }
                
                // Step 4: Clean up old data that wasn't updated in this download operation
                _logger?.LogDebug("{Prefix} Step 4: Cleaning up old {Operator} data (older than 1 day)", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                
                OnProgressChanged(0.97, "Cleaning up old data...");
                try 
                {
                    await CleanUpOldDataAsync(_currentDownloadTimestamp, cancellationToken);
                    OnProgressChanged(1.0, "Cleanup completed");
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "{Prefix} Error cleaning up old {Operator} data", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                    OnProgressChanged(1.0, "Cleanup failed, but download complete");
                }
                
                // Signal explicit completion to trigger UI refresh
                OnProgressChanged(1.0, "DOWNLOAD_COMPLETE");
                
                _logger?.LogInformation("{Prefix} {Separator}", LOG_PREFIX, LOG_SEPARATOR);
                _logger?.LogInformation("{Prefix} {Operator} data download completed successfully", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                _logger?.LogInformation("{Prefix} {Separator}", LOG_PREFIX, LOG_SEPARATOR);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "{Prefix} Error in {Operator} data download process", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                OnProgressChanged(1.0, "Error during download");
                // Signal completion even though there was an error
                OnProgressChanged(1.0, "DOWNLOAD_COMPLETE", false);
            }
        }

        /// <summary>
        /// Download all KMB stops
        /// </summary>
        private async Task DownloadStopsAsync(CancellationToken cancellationToken = default)
        {
            _logger?.LogInformation("{Prefix} Downloading {Operator} stops...", LOG_PREFIX, LOG_TRANSPORT_TYPE);
            
            try
            {
                _logger?.LogDebug("Calling KMB stops API endpoint");
                OnProgressChanged(0.25, "Fetching stops data...");
                
                var stopData = await _httpClient.GetFromJsonAsync<KmbStopResponse>(
                    "https://data.etabus.gov.hk/v1/transport/kmb/stop",
                    HttpClientUtility.PRIORITY_LOW);
                    
                if (stopData?.Data == null || stopData.Data.Count == 0)
                {
                    _logger?.LogWarning("KMB stops API returned empty data collection");
                    OnProgressChanged(0.3, "No stops data found");
                    return;
                }
                
                // Process the stop data
                var kmbStops = stopData.Data;
                
                _logger?.LogInformation("Retrieved {count} KMB stops", kmbStops.Count);
                
                List<TransportStop> stops = new List<TransportStop>(kmbStops.Count);
                
                foreach (var kmbStop in kmbStops)
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;
                        
                    // Parse coordinates
                    double.TryParse(kmbStop.Lat, out double latitude);
                    double.TryParse(kmbStop.Long, out double longitude);
                    
                    var stop = new TransportStop
                    {
                        StopId = kmbStop.Stop,
                        Operator = TransportOperator.KMB,
                        Id = $"KMB_{kmbStop.Stop}",
                        NameEn = kmbStop.NameEn,
                        NameZhHant = kmbStop.NameTc,
                        NameZhHans = kmbStop.NameSc,
                        Latitude = latitude,
                        Longitude = longitude,
                        LastUpdated = DateTime.UtcNow
                    };
                    
                    // Generate geohashes for location-based filtering (requires NGeoHash)
                    if (stop.Latitude != 0 && stop.Longitude != 0)
                    {
                        try
                        {
                            // Generate geohashes with different precision for different search scenarios
                            stop.GeoHash6 = NGeoHash.GeoHash.Encode(stop.Latitude, stop.Longitude, 6);
                            stop.GeoHash7 = NGeoHash.GeoHash.Encode(stop.Latitude, stop.Longitude, 7);
                            stop.GeoHash8 = NGeoHash.GeoHash.Encode(stop.Latitude, stop.Longitude, 8);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "Error generating geohash for stop {stopId}", stop.StopId);
                        }
                    }
                    
                    stops.Add(stop);
                }
                
                // Use DataService first, then fallback to direct LiteDB if needed
                if (stops.Count > 0)
                {
                    try 
                    {
                        // Break into smaller batches for better performance
                        const int stopBatchSize = 300; // Use smaller batches for stops too
                        
                        _logger?.LogInformation("Saving {count} stops to database in batches of {batchSize}", 
                            stops.Count, stopBatchSize);
                            
                        for (int i = 0; i < stops.Count; i += stopBatchSize)
                        {
                            if (cancellationToken.IsCancellationRequested)
                                return;
                                
                            var batch = stops.Skip(i).Take(stopBatchSize).ToList();
                            _databaseService.BulkInsert("TransportStops", batch);
                            
                            // Report progress
                            double stopProgress = 0.25 + (0.1 * (i / (double)stops.Count));
                            OnProgressChanged(stopProgress, $"Saved {i + batch.Count}/{stops.Count} stops...");
                            
                            // Small delay to allow other operations
                            await Task.Delay(10, cancellationToken);
                        }
                        
                        _logger?.LogInformation("Saved {count} stops to database", stops.Count);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error saving stops to database");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error downloading KMB stops");
                OnProgressChanged(0.3, "Error downloading stops data");
            }
        }

        /// <summary>
        /// Download all KMB routes
        /// </summary>
        private async Task DownloadRoutesAsync(CancellationToken cancellationToken = default)
        {
            _logger?.LogInformation("{Prefix} Downloading {Operator} routes...", LOG_PREFIX, LOG_TRANSPORT_TYPE);
            
            try
            {
                _logger?.LogDebug("Calling KMB routes API endpoint");
                OnProgressChanged(0.35, "Fetching routes data...");
                
                var routeData = await _httpClient.GetFromJsonAsync<KmbRouteResponse>(
                    "https://data.etabus.gov.hk/v1/transport/kmb/route/",
                    HttpClientUtility.PRIORITY_LOW);
                    
                if (routeData?.Data == null || routeData.Data.Count == 0)
                {
                    _logger?.LogWarning("KMB routes API returned empty data collection");
                    OnProgressChanged(0.4, "No routes data found");
                    return;
                }
                
                // Process the route data
                var kmbRoutes = routeData.Data;
                
                _logger?.LogInformation("Retrieved {count} KMB routes", kmbRoutes.Count);
                
                List<TransportRoute> routes = new List<TransportRoute>(kmbRoutes.Count);
                
                foreach (var kmbRoute in kmbRoutes)
                {
                    if (cancellationToken.IsCancellationRequested)
                        return;
                        
                    var route = new TransportRoute
                    {
                        RouteNumber = kmbRoute.Route,
                        Bound = kmbRoute.Bound,
                        ServiceType = kmbRoute.ServiceType,
                        OriginEn = kmbRoute.OrigEn,
                        OriginZhHant = kmbRoute.OrigTc,
                        OriginZhHans = kmbRoute.OrigSc,
                        DestinationEn = kmbRoute.DestEn,
                        DestinationZhHant = kmbRoute.DestTc,
                        DestinationZhHans = kmbRoute.DestSc,
                        Operator = TransportOperator.KMB,
                        Id = $"KMB_{kmbRoute.Route}_{kmbRoute.Bound}_{kmbRoute.ServiceType}",
                        Type = TransportType.Bus,
                        LastUpdated = DateTime.UtcNow
                    };
                    
                    routes.Add(route);
                }
                
                // Save to database using DatabaseService
                if (routes.Count > 0)
                {
                    try 
                    {
                        // Break into smaller batches for better performance
                        const int routeBatchSize = 300; // Smaller batches are faster for routes
                        
                        _logger?.LogInformation("Saving {count} routes to database in batches of {batchSize}", 
                            routes.Count, routeBatchSize);
                            
                        for (int i = 0; i < routes.Count; i += routeBatchSize)
                        {
                            if (cancellationToken.IsCancellationRequested)
                                return;
                                
                            var batch = routes.Skip(i).Take(routeBatchSize).ToList();
                            _databaseService.BulkInsert("TransportRoutes", batch);
                            
                            // Report progress
                            double routeProgress = 0.4 + (0.1 * (i / (double)routes.Count));
                            OnProgressChanged(routeProgress, $"Saved {i + batch.Count}/{routes.Count} routes...");
                            
                            // Small delay to allow other operations
                            await Task.Delay(10, cancellationToken);
                        }
                        
                        _logger?.LogInformation("Saved {count} routes to database", routes.Count);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error saving routes to database");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error downloading KMB routes");
                OnProgressChanged(0.4, "Error downloading routes data");
            }
        }

        /// <summary>
        /// Download route-stops data for all routes
        /// </summary>
        private async Task DownloadRouteStopsAsync(CancellationToken cancellationToken = default)
        {
            _logger?.LogInformation("{Prefix} Downloading KMB route-stops...", LOG_PREFIX);
            
            const int maxRetries = 5;
            int retryCount = 0;
            const int baseTimeoutSeconds = 60; // Increased timeout for large dataset
            
            while (retryCount <= maxRetries)
            {
                try
                {
                    _logger?.LogDebug("Calling KMB route-stops API endpoint");
                    OnProgressChanged(0.6, "Fetching route stops data...");
                    
                    // Create a CancellationTokenSource with the increased timeout
                    using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(baseTimeoutSeconds * (retryCount + 1)));
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
                    
                    // Make the API call with the custom timeout
                    var routeStopData = await _httpClient.GetFromJsonAsync<KmbRouteStopResponse>(
                        "https://data.etabus.gov.hk/v1/transport/kmb/route-stop",
                        HttpClientUtility.PRIORITY_LOW,
                        null, // headers parameter
                        linkedCts.Token);
                        
                    if (routeStopData?.Data == null || routeStopData.Data.Count == 0)
                    {
                        _logger?.LogWarning("KMB route-stops API returned empty data collection");
                        OnProgressChanged(0.7, "No route stops data found");
                        return;
                    }
                    
                    _logger?.LogInformation("Retrieved {count} KMB route-stops", routeStopData.Data.Count);
                    
                    // Process route-stop data
                    List<RouteStopRelation> routeStops = new List<RouteStopRelation>();
                    
                    foreach (var rs in routeStopData.Data)
                    {
                        if (cancellationToken.IsCancellationRequested)
                            return;
                        
                        // Skip invalid data
                        if (string.IsNullOrEmpty(rs.Route) || string.IsNullOrEmpty(rs.Stop) || string.IsNullOrEmpty(rs.Bound))
                            continue;
                        
                        // Parse the sequence number
                        int.TryParse(rs.Seq, out int sequence);
                        
                        var routeStop = new RouteStopRelation
                        {
                            RouteId = $"KMB_{rs.Route}_{rs.Bound}_{rs.ServiceType}",
                            StopId = $"KMB_{rs.Stop}",
                            Sequence = sequence,
                            Direction = rs.Bound,
                            LastUpdated = DateTime.UtcNow
                        };
                        
                        routeStops.Add(routeStop);
                    }
                    
                    // Save to database in smaller batches
                    if (routeStops.Count > 0)
                    {
                        try
                        {
                            // Break into batches of 500 for better performance
                            const int batchSize = 500;
                            for (int i = 0; i < routeStops.Count; i += batchSize)
                            {
                                if (cancellationToken.IsCancellationRequested)
                                    return;
                                    
                                var batch = routeStops.Skip(i).Take(batchSize).ToList();
                                _databaseService.BulkInsert("RouteStopRelations", batch);
                                
                                // Report progress periodically
                                double progress = 0.7 + (0.1 * (i / (double)routeStops.Count));
                                OnProgressChanged(progress, $"Saved {i + batch.Count}/{routeStops.Count} route-stops...");
                            }
                            
                            _logger?.LogInformation("Saved {count} route-stops to database", routeStops.Count);
                            OnProgressChanged(0.8, $"Processed {routeStops.Count} route-stops");
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "Error saving route-stops to database");
                        }
                    }
                    
                    // Success! Exit the retry loop
                    break;
                }
                catch (OperationCanceledException ex)
                {
                    retryCount++;
                    if (retryCount <= maxRetries)
                    {
                        int delayMs = 1000 * retryCount; // Exponential backoff
                        _logger?.LogWarning("Request timed out downloading KMB route-stops (attempt {current}/{max}). Retrying in {delay}ms...", 
                            retryCount, maxRetries, delayMs);
                        OnProgressChanged(0.6, $"Retrying route-stops download (attempt {retryCount}/{maxRetries})...");
                        await Task.Delay(delayMs, cancellationToken);
                    }
                    else
                    {
                        _logger?.LogError(ex, "Request timed out downloading KMB route-stops after {attempts} attempts", maxRetries);
                        OnProgressChanged(0.7, "Error downloading route stops - timeout");
                        return;
                    }
                }
                catch (HttpRequestException ex)
                {
                    retryCount++;
                    if (retryCount <= maxRetries)
                    {
                        int delayMs = 1000 * retryCount; // Exponential backoff
                        _logger?.LogWarning(ex, "Network error downloading KMB route-stops (attempt {current}/{max}). Retrying in {delay}ms...", 
                            retryCount, maxRetries, delayMs);
                        OnProgressChanged(0.6, $"Retrying route-stops download (attempt {retryCount}/{maxRetries})...");
                        await Task.Delay(delayMs, cancellationToken);
                    }
                    else
                    {
                        _logger?.LogError(ex, "Network error downloading KMB route-stops after {attempts} attempts", maxRetries);
                        OnProgressChanged(0.7, "Error downloading route stops - network error");
                        return;
                    }
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount <= maxRetries)
                    {
                        int delayMs = 1000 * retryCount; // Exponential backoff
                        _logger?.LogWarning(ex, "Error downloading KMB route-stops (attempt {current}/{max}). Retrying in {delay}ms...", 
                            retryCount, maxRetries, delayMs);
                        OnProgressChanged(0.6, $"Retrying route-stops download (attempt {retryCount}/{maxRetries})...");
                        await Task.Delay(delayMs, cancellationToken);
                    }
                    else
                    {
                        _logger?.LogError(ex, "Error downloading KMB route-stops after {attempts} attempts", maxRetries);
                        OnProgressChanged(0.7, "Error downloading route stops");
                        return;
                    }
                }
            }
        }

        /// <summary>
        /// Cleans up old data that wasn't updated during the current download operation
        /// </summary>
        private async Task CleanUpOldDataAsync(DateTime currentTimestamp, CancellationToken cancellationToken)
        {
            _logger?.LogDebug("{Prefix} Step 4: Cleaning up old KMB data (older than 1 day)", LOG_PREFIX);
            DateTime cutoffDate = currentTimestamp - DATA_CLEANUP_AGE;
            _logger?.LogInformation("{Prefix} Cleaning up KMB data older than {cutoffDate}", LOG_PREFIX, cutoffDate);
            
            // We need to implement cleanup directly with LiteDbService
            try
            {
                // Clean up using LiteDbService's methods
                await Task.Run(() => {
                    try
                    {
                        // Get TransportRoutes collection and update LastUpdated for old records
                        var routes = _databaseService.GetAllRecords<TransportRoute>("TransportRoutes")
                            .Where(r => r.Operator == TransportOperator.KMB && r.LastUpdated < cutoffDate)
                            .ToList();
                        
                        if (routes.Any())
                        {
                            _logger?.LogInformation("Found {count} outdated KMB routes to update timestamps", routes.Count);
                            
                            // Update all routes to refresh their LastUpdated timestamp
                            foreach (var route in routes)
                            {
                                route.LastUpdated = DateTime.UtcNow;
                                _databaseService.UpdateRecord("TransportRoutes", route);
                            }
                        }
                        
                        // Clear any caches
                        _databaseService.ClearRouteStopsCache();
                        
                        _logger?.LogInformation("Cleanup completed successfully");
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error during data cleanup");
                    }
                }, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _logger?.LogInformation("Cleanup operation was cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error cleaning up old data");
            }
        }
    }

    #region API Response Models

    public class KmbStopResponse
    {
        public List<KmbStop> Data { get; set; } = new List<KmbStop>();
    }

    public class KmbStop
    {
        [JsonPropertyName("stop")]
        public string Stop { get; set; } = string.Empty;
        
        [JsonPropertyName("name_en")]
        public string NameEn { get; set; } = string.Empty;
        
        [JsonPropertyName("name_tc")]
        public string NameTc { get; set; } = string.Empty;
        
        [JsonPropertyName("name_sc")]
        public string NameSc { get; set; } = string.Empty;
        
        [JsonPropertyName("lat")]
        public string Lat { get; set; } = string.Empty;
        
        [JsonPropertyName("long")]
        public string Long { get; set; } = string.Empty;
    }

    public class KmbRouteResponse
    {
        public List<KmbRoute> Data { get; set; } = new List<KmbRoute>();
    }

    public class KmbRoute
    {
        [JsonPropertyName("route")]
        public string Route { get; set; } = string.Empty;
        
        [JsonPropertyName("bound")]
        public string Bound { get; set; } = string.Empty;
        
        [JsonPropertyName("service_type")]
        public string ServiceType { get; set; } = string.Empty;
        
        [JsonPropertyName("orig_en")]
        public string OrigEn { get; set; } = string.Empty;
        
        [JsonPropertyName("orig_tc")]
        public string OrigTc { get; set; } = string.Empty;
        
        [JsonPropertyName("orig_sc")]
        public string OrigSc { get; set; } = string.Empty;
        
        [JsonPropertyName("dest_en")]
        public string DestEn { get; set; } = string.Empty;
        
        [JsonPropertyName("dest_tc")]
        public string DestTc { get; set; } = string.Empty;
        
        [JsonPropertyName("dest_sc")]
        public string DestSc { get; set; } = string.Empty;
    }

    public class KmbRouteStopResponse
    {
        public List<KmbRouteStop> Data { get; set; } = new List<KmbRouteStop>();
    }

    public class KmbRouteStop
    {
        [JsonPropertyName("route")]
        public string Route { get; set; } = string.Empty;
        
        [JsonPropertyName("bound")]
        public string Bound { get; set; } = string.Empty;
        
        [JsonPropertyName("service_type")]
        public string ServiceType { get; set; } = string.Empty;
        
        [JsonPropertyName("seq")]
        public string Seq { get; set; } = string.Empty;
        
        [JsonPropertyName("stop")]
        public string Stop { get; set; } = string.Empty;
    }

    // Helper classes moved inside region
    internal class RouteKeyMapping
    {
        public string Id { get; set; } = string.Empty;
        public string RouteNumber { get; set; } = string.Empty;
        public string ServiceType { get; set; } = string.Empty;
        public string Bound { get; set; } = string.Empty;
    }
    
    internal class StopIdMapping
    {
        public string Id { get; set; } = string.Empty;
        public string StopId { get; set; } = string.Empty;
    }

    #endregion
} 