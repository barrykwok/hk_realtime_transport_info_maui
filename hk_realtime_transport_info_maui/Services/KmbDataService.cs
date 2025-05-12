using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using hk_realtime_transport_info_maui.Models;
using SQLite;
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
        private readonly DatabaseService _databaseService;
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
        
        public KmbDataService(DatabaseService databaseService, HttpClientUtility httpClient, ILogger<KmbDataService> logger)
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
                var db = _databaseService.GetDatabase();
                
                // Check if there are any routes for this operator
                var query = "SELECT * FROM TransportRoute WHERE Operator = ? ORDER BY LastUpdated DESC LIMIT 1";
                var routes = db.Query<TransportRoute>(query, (int)operatorType);
                bool dataExists = routes.Count > 0;
                
                if (!dataExists)
                {
                    // Double-check with a count query which may be more reliable
                    try 
                    {
                        int count = db.ExecuteScalar<int>("SELECT COUNT(*) FROM TransportRoute WHERE Operator = ?", (int)operatorType);
                        dataExists = count > 0;
                        
                        if (dataExists)
                        {
                            _logger?.LogWarning("First query showed no data but count query found {count} routes", count);
                        }
                    }
                    catch 
                    {
                        // If this query fails too, continue with the original result
                    }
                    
                    if (!dataExists)
                    {
                        _logger?.LogInformation("No data found for operator {operator}", operatorType);
                        
                        // Store in cache
                        _dataFreshnessCache[operatorType] = (false, false, DateTime.UtcNow);
                        return (false, false);
                    }
                }
                
                // Check data freshness (using the newest route's timestamp)
                var newestRoute = routes.FirstOrDefault();
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
            catch (SQLiteException ex) when (ex.Message.Contains("malformed") || ex.Message.Contains("corrupt"))
            {
                _logger?.LogError(ex, "Database corruption detected during freshness check for {operator}", operatorType);
                
                // Try to repair the database and retry once
                try
                {
                    var databaseService = IPlatformApplication.Current?.Services?.GetService<DatabaseService>();
                    if (databaseService != null)
                    {
                        bool repaired = databaseService.CheckAndRepairDatabase();
                        if (repaired)
                        {
                            _logger?.LogInformation("Database repaired, retrying freshness check");
                            
                            // Retry the query after repair
                            var db = _databaseService.GetDatabase();
                            int count = db.ExecuteScalar<int>("SELECT COUNT(*) FROM TransportRoute WHERE Operator = ?", (int)operatorType);
                            bool exists = count > 0;
                            
                            // Store result in cache
                            _dataFreshnessCache[operatorType] = (exists, true, DateTime.UtcNow);
                            return (exists, true);
                        }
                    }
                }
                catch (Exception repairEx)
                {
                    _logger?.LogError(repairEx, "Failed to repair database during freshness check");
                }
                
                // Fall through to the general error case
                _logger?.LogWarning("Database error occurred. Assuming data exists and is fresh to prevent unnecessary download");
                _dataFreshnessCache[operatorType] = (true, true, DateTime.UtcNow);
                return (true, true);
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
            if (_logger != null)
            {
                _logger.LogInformation("{Prefix} Downloading {Operator} stops...", LOG_PREFIX, LOG_TRANSPORT_TYPE);
            }
            
            try
            {
                if (_logger != null)
                {
                    _logger.LogDebug("Calling KMB stops API endpoint");
                }
                
                var stopData = await _httpClient.GetFromJsonAsync<KmbStopResponse>(
                    "https://data.etabus.gov.hk/v1/transport/kmb/stop", 
                    HttpClientUtility.PRIORITY_LOW);
                
                if (stopData?.Data == null || stopData.Data.Count == 0)
                {
                    if (_logger != null)
                    {
                        _logger.LogWarning("KMB stops API returned empty data collection");
                    }
                    return;
                }
                
                if (_logger != null)
                {
                    _logger.LogInformation("Retrieved {count} KMB stops", stopData.Data.Count);
                }
                
                // Get a single database connection that will be used throughout this operation
                var db = _databaseService.GetDatabase();
                
                // First, create a lookup of existing stops to avoid unnecessary database queries
                if (_logger != null)
                {
                    _logger.LogDebug("Creating lookup of existing stops");
                }
                var existingStopsLookup = db.Table<TransportStop>().ToList().ToDictionary(s => s.StopId);
                
                var stopsToUpsert = new List<TransportStop>();
                int newStopsCount = 0;
                int existingStopsCount = 0;
                
                // Process each stop
                foreach (var stop in stopData.Data)
                {
                    // Periodically check for cancellation to be responsive
                    if (cancellationToken.IsCancellationRequested)
                    {
                        if (_logger != null)
                        {
                            _logger.LogInformation("Stop download cancelled");
                        }
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    
                    string stopId = stop.Stop;
                    if (string.IsNullOrEmpty(stopId)) continue;
                    
                    if (existingStopsLookup.TryGetValue(stopId, out var existingStop))
                    {
                        // Update existing stop
                        existingStop.NameEn = stop.NameEn ?? string.Empty;
                        existingStop.NameZhHant = stop.NameTc ?? string.Empty;
                        existingStop.NameZhHans = stop.NameSc ?? string.Empty;
                        double.TryParse(stop.Lat, out double lat);
                        existingStop.Latitude = lat;
                        double.TryParse(stop.Long, out double lon);
                        existingStop.Longitude = lon;
                        existingStop.Operator = TransportOperator.KMB;
                        // Set LastUpdated to current download timestamp
                        existingStop.LastUpdated = _currentDownloadTimestamp;
                        
                        // Update geohashes if coordinates are valid
                        if (Math.Abs(lat) > 0.001 && Math.Abs(lon) > 0.001)
                        {
                            existingStop.GeoHash6 = GeoHash.Encode(lat, lon, 6);
                            existingStop.GeoHash7 = GeoHash.Encode(lat, lon, 7);
                            existingStop.GeoHash8 = GeoHash.Encode(lat, lon, 8);
                            existingStop.GeoHash9 = GeoHash.Encode(lat, lon, 9);
                        }
                        
                        stopsToUpsert.Add(existingStop);
                        existingStopsCount++;
                    }
                    else
                    {
                        // Create new stop
                        var newStop = new TransportStop
                        {
                            StopId = stopId,
                            NameEn = stop.NameEn ?? string.Empty,
                            NameZhHant = stop.NameTc ?? string.Empty,
                            NameZhHans = stop.NameSc ?? string.Empty,
                            Operator = TransportOperator.KMB,
                            // Set LastUpdated to current download timestamp
                            LastUpdated = _currentDownloadTimestamp
                        };
                        
                        double.TryParse(stop.Lat, out double lat);
                        newStop.Latitude = lat;
                        double.TryParse(stop.Long, out double lon);
                        newStop.Longitude = lon;
                        
                        // Generate geohashes if coordinates are valid
                        if (Math.Abs(lat) > 0.001 && Math.Abs(lon) > 0.001)
                        {
                            newStop.GeoHash6 = GeoHash.Encode(lat, lon, 6);
                            newStop.GeoHash7 = GeoHash.Encode(lat, lon, 7);
                            newStop.GeoHash8 = GeoHash.Encode(lat, lon, 8);
                            newStop.GeoHash9 = GeoHash.Encode(lat, lon, 9);
                        }
                        
                        stopsToUpsert.Add(newStop);
                        newStopsCount++;
                    }
                    
                    // Batch insert to reduce database operations
                    if (stopsToUpsert.Count >= STOP_BATCH_SIZE)
                    {
                        if (_logger != null)
                        {
                            _logger.LogDebug("Upserting batch of {count} stops", stopsToUpsert.Count);
                        }
                        await Task.Run(() => 
                        {
                            try
                            {
                                // Try to start a transaction - if one is already in progress, the exception will be caught
                                db.BeginTransaction();
                                bool transactionStarted = true;
                                
                                try
                                {
                                    foreach (var s in stopsToUpsert)
                                    {
                                        // InsertOrReplace combines Insert and Update functionality
                                        db.InsertOrReplace(s);
                                    }
                                    
                                    if (transactionStarted)
                                    {
                                        db.Commit();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    if (transactionStarted)
                                    {
                                        try
                                        {
                                            db.Rollback();
                                        }
                                        catch
                                        {
                                            // Ignore rollback errors
                                        }
                                    }
                                    
                                    if (_logger != null)
                                    {
                                        _logger.LogError(ex, "Error upserting stops batch");
                                    }
                                }
                            }
                            catch (SQLiteException ex) when (ex.Message.Contains("already in a transaction"))
                            {
                                // If transaction is already in progress, just execute statements without transaction
                                _logger?.LogWarning("Transaction already in progress, continuing without transaction");
                                try
                                {
                                    foreach (var s in stopsToUpsert)
                                    {
                                        // InsertOrReplace combines Insert and Update functionality
                                        db.InsertOrReplace(s);
                                    }
                                }
                                catch (Exception innerEx)
                                {
                                    if (_logger != null)
                                    {
                                        _logger.LogError(innerEx, "Error upserting stops without transaction");
                                    }
                                }
                            }
                        }, cancellationToken);
                        
                        // Allow other tasks to run and UI to remain responsive
                        
                        stopsToUpsert.Clear();
                    }
                }
                
                // Insert any remaining stops
                if (stopsToUpsert.Count > 0)
                {
                    if (_logger != null)
                    {
                        _logger.LogDebug("Upserting final batch of {count} stops", stopsToUpsert.Count);
                    }
                    await Task.Run(() => 
                    {
                        try
                        {
                            // Try to start a transaction - if one is already in progress, the exception will be caught
                            db.BeginTransaction();
                            bool transactionStarted = true;
                            
                            try
                            {
                                foreach (var s in stopsToUpsert)
                                {
                                    db.InsertOrReplace(s);
                                }
                                
                                if (transactionStarted)
                                {
                                    db.Commit();
                                }
                            }
                            catch (Exception ex)
                            {
                                if (transactionStarted)
                                {
                                    try
                                    {
                                        db.Rollback();
                                    }
                                    catch
                                    {
                                        // Ignore rollback errors
                                    }
                                }
                                
                                if (_logger != null)
                                {
                                    _logger.LogError(ex, "Error upserting final stops batch");
                                }
                            }
                        }
                        catch (SQLiteException ex) when (ex.Message.Contains("already in a transaction"))
                        {
                            // If transaction is already in progress, just execute statements without transaction
                            _logger?.LogWarning("Transaction already in progress, continuing without transaction");
                            try
                            {
                                foreach (var s in stopsToUpsert)
                                {
                                    db.InsertOrReplace(s);
                                }
                            }
                            catch (Exception innerEx)
                            {
                                if (_logger != null)
                                {
                                    _logger.LogError(innerEx, "Error upserting final stops without transaction");
                                }
                            }
                        }
                    }, cancellationToken);
                }
                
                if (_logger != null)
                {
                    _logger.LogInformation("Completed downloading {count} KMB stops (new: {new}, existing: {existing})", 
                        stopData.Data.Count, newStopsCount, existingStopsCount);
                }
            }
            catch (TaskCanceledException)
            {
                if (_logger != null)
                {
                    _logger.LogInformation("Stops download was cancelled");
                }
                throw;
            }
            catch (Exception ex)
            {
                if (_logger != null)
                {
                    _logger.LogError(ex, "Error downloading KMB stops");
                }
                throw;
            }
        }

        /// <summary>
        /// Download all KMB routes
        /// </summary>
        private async Task DownloadRoutesAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("{Prefix} Downloading {Operator} routes...", LOG_PREFIX, LOG_TRANSPORT_TYPE);
            
            try
            {
                _logger.LogDebug("Calling KMB routes API endpoint");
                OnProgressChanged(0.35, "Fetching routes data...");
                
                var routeData = await _httpClient.GetFromJsonAsync<KmbRouteResponse>(
                    "https://data.etabus.gov.hk/v1/transport/kmb/route/",
                    HttpClientUtility.PRIORITY_LOW);
                    
                if (routeData?.Data == null || routeData.Data.Count == 0)
                {
                    _logger.LogWarning("KMB routes API returned empty data collection");
                    return;
                }
                
                _logger.LogInformation("Retrieved {count} KMB routes", routeData.Data.Count);
                
                // Get a single database connection that will be used throughout this operation
                var db = _databaseService.GetDatabase();
                
                // Create a lookup of existing routes to avoid individual database queries
                _logger.LogDebug("Creating lookup of existing routes");
                var allExistingRoutes = db.Table<TransportRoute>().ToList();
                
                // Create a lookup dictionary for faster matching using Key property
                var routeLookup = new Dictionary<string, TransportRoute>();
                var routeLookupByNumberAndBound = new Dictionary<string, List<TransportRoute>>();
                
                foreach (var route in allExistingRoutes)
                {
                    // Primary key lookup with full key - use a composite key for direct lookup
                    string routeKey = $"{route.RouteNumber}|{route.ServiceType}|{route.Bound}";
                    routeLookup[routeKey] = route;
                    
                    // Secondary key lookup
                    string simpleKey = $"{route.RouteNumber}|{route.Bound}";
                    if (!routeLookupByNumberAndBound.TryGetValue(simpleKey, out var routes))
                    {
                        routes = new List<TransportRoute>();
                        routeLookupByNumberAndBound[simpleKey] = routes;
                    }
                    routes.Add(route);
                }
                
                OnProgressChanged(0.45, $"Processing {routeData.Data.Count} routes...");
                
                // Use concurrent collection for thread safety
                var routesToUpsert = new ConcurrentBag<TransportRoute>();
                
                // Use parallelization for better performance with max degree of parallelism to avoid overwhelming
                Parallel.ForEach(routeData.Data, 
                    new ParallelOptions { MaxDegreeOfParallelism = Math.Min(Environment.ProcessorCount, 4) }, 
                    route =>
                {
                    // Ensure ServiceType is never null or empty for matching
                    string serviceType = !string.IsNullOrEmpty(route.ServiceType) ? route.ServiceType : "1";
                    
                    // Try to find the matching route in lookup
                    TransportRoute? existingRoute = null;
                    
                    // Try direct match first
                    string key = $"{route.Route}|{serviceType}|{route.Bound}";
                    if (routeLookup.TryGetValue(key, out var directMatch))
                    {
                        existingRoute = directMatch;
                    }
                    // Try with default service type as fallback
                    else if (serviceType != "1")
                    {
                        string defaultKey = $"{route.Route}|1|{route.Bound}";
                        routeLookup.TryGetValue(defaultKey, out existingRoute);
                    }
                    // Try by route number and bound as last resort
                    if (existingRoute == null)
                    {
                        string simpleKey = $"{route.Route}|{route.Bound}";
                        if (routeLookupByNumberAndBound.TryGetValue(simpleKey, out var candidates) && candidates.Count > 0)
                        {
                            existingRoute = candidates[0];
                        }
                    }
                    
                    if (existingRoute != null)
                    {
                        // Update existing route fields
                        existingRoute.OriginEn = route.OrigEn ?? string.Empty;
                        existingRoute.OriginZhHant = route.OrigTc ?? string.Empty;
                        existingRoute.OriginZhHans = route.OrigSc ?? string.Empty;
                        existingRoute.DestinationEn = route.DestEn ?? string.Empty;
                        existingRoute.DestinationZhHant = route.DestTc ?? string.Empty;
                        existingRoute.DestinationZhHans = route.DestSc ?? string.Empty;
                        existingRoute.ServiceType = serviceType;
                        existingRoute.Type = TransportType.Bus;
                        existingRoute.Operator = TransportOperator.KMB;
                        existingRoute.LastUpdated = _currentDownloadTimestamp;
                        
                        routesToUpsert.Add(existingRoute);
                    }
                    else
                    {
                        // Create new route
                        var newRoute = new TransportRoute
                        {
                            RouteNumber = route.Route ?? string.Empty,
                            RouteName = $"{route.OrigEn ?? string.Empty} - {route.DestEn ?? string.Empty}",
                            OriginEn = route.OrigEn ?? string.Empty,
                            OriginZhHant = route.OrigTc ?? string.Empty,
                            OriginZhHans = route.OrigSc ?? string.Empty,
                            DestinationEn = route.DestEn ?? string.Empty,
                            DestinationZhHant = route.DestTc ?? string.Empty,
                            DestinationZhHans = route.DestSc ?? string.Empty,
                            ServiceType = serviceType,
                            Bound = route.Bound ?? string.Empty,
                            Type = TransportType.Bus,
                            Operator = TransportOperator.KMB,
                            LastUpdated = _currentDownloadTimestamp
                        };
                        routesToUpsert.Add(newRoute);
                    }
                });
                
                OnProgressChanged(0.5, "Saving route data...");
                
                // Convert to list for batch processing
                var routesToProcess = routesToUpsert.ToList();
                
                // Use larger batch size for better performance
                const int BATCH_SIZE = ROUTE_BATCH_SIZE;
                
                // Insert/update routes in batches using upsert
                if (routesToProcess.Count > 0)
                {
                    _logger.LogDebug("Processing {count} routes in batches", routesToProcess.Count);
                    
                    int totalBatches = (routesToProcess.Count + BATCH_SIZE - 1) / BATCH_SIZE;
                    for (int i = 0; i < totalBatches; i++)
                    {
                        int startIdx = i * BATCH_SIZE;
                        int count = Math.Min(BATCH_SIZE, routesToProcess.Count - startIdx);
                        var batch = routesToProcess.GetRange(startIdx, count);
                        
                        try
                        {
                            // Try to start a transaction - if one is already in progress, the exception will be caught
                            db.BeginTransaction();
                            bool transactionStarted = true;
                            
                            try
                            {
                                foreach (var route in batch)
                                {
                                    db.InsertOrReplace(route);
                                }
                                
                                if (transactionStarted)
                                {
                                    db.Commit();
                                }
                                
                                double progress = 0.5 + (0.1 * (i + 1) / totalBatches);
                                OnProgressChanged(progress, $"Saving routes ({i+1}/{totalBatches})...");
                            }
                            catch (Exception ex)
                            {
                                if (transactionStarted)
                                {
                                    try
                                    {
                                        db.Rollback();
                                    }
                                    catch
                                    {
                                        // Ignore rollback errors
                                    }
                                }
                                
                                _logger?.LogError(ex, "Error processing routes batch");
                                throw;
                            }
                        }
                        catch (SQLiteException ex) when (ex.Message.Contains("already in a transaction"))
                        {
                            // If transaction is already in progress, just execute statements without transaction
                            _logger?.LogWarning("Transaction already in progress, continuing without transaction");
                            try
                            {
                                foreach (var route in batch)
                                {
                                    db.InsertOrReplace(route);
                                }
                                
                                double progress = 0.5 + (0.1 * (i + 1) / totalBatches);
                                OnProgressChanged(progress, $"Saving routes ({i+1}/{totalBatches})...");
                            }
                            catch (Exception innerEx)
                            {
                                _logger?.LogError(innerEx, "Error processing routes without transaction");
                                throw;
                            }
                        }
                    }
                    
                    // Count new and existing routes for logging
                    int newRoutes = routesToProcess.Count(r => !allExistingRoutes.Any(er => er.Id == r.Id));
                    int updatedRoutes = routesToProcess.Count - newRoutes;
                    
                    _logger?.LogInformation(newRoutes > 0 
                        ? "Inserted {count} new routes" 
                        : "Updated {count} existing routes", 
                        newRoutes > 0 ? newRoutes : updatedRoutes);
                }
                
                _logger?.LogDebug("Completed downloading and processing KMB routes");
                OnProgressChanged(0.6, "Routes processed successfully");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error downloading KMB routes: {message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Download route-stops data for all routes
        /// </summary>
        private async Task DownloadRouteStopsAsync(CancellationToken cancellationToken = default)
        {
            _logger?.LogDebug("Step 3: Downloading route-stops");
            _logger?.LogInformation("[KmbDataService] Downloading KMB route-stops...");
            
            try
            {
                _logger?.LogDebug("Calling KMB route-stops API endpoint");
                
                // Handle corrupted database recovery
                var exceptionHandler = IPlatformApplication.Current?.Services?.GetService<ExceptionHandlingService>();
                var databaseService = IPlatformApplication.Current?.Services?.GetService<DatabaseService>();
                
                // Check if database needs repair before starting
                try
                {
                    var testQuery = _databaseService.GetDatabase().ExecuteScalar<int>("SELECT COUNT(*) FROM sqlite_master");
                }
                catch (SQLiteException ex) when (ex.Message.Contains("malformed") || ex.Message.Contains("corrupt"))
                {
                    _logger?.LogError(ex, "Database corruption detected before route-stops download");
                    
                    if (databaseService != null)
                    {
                        bool repaired = databaseService.CheckAndRepairDatabase();
                        if (!repaired)
                        {
                            _logger?.LogError("Unable to repair database, route-stops download may fail");
                        }
                    }
                }
                
                var response = await _httpClient.GetFromJsonAsync<KmbRouteStopResponse>(
                    "https://data.etabus.gov.hk/v1/transport/kmb/route-stop");
                    
                if (response?.Data == null)
                {
                    _logger?.LogWarning("No route-stops data returned from API");
                    return;
                }
                    
                _logger?.LogInformation("Retrieved {count} KMB route-stops", response.Data.Count);
                
                // Create lookup dictionaries for routes and stops
                _logger?.LogDebug("Creating lookup dictionaries for routes and stops");
                
                var routeDictionary = _databaseService.GetAllRecords<TransportRoute>("routes")
                    .ToDictionary(r => r.Id, r => r);
                    
                var stopDictionary = _databaseService.GetAllRecords<TransportStop>("stops")
                    .ToDictionary(s => s.Id, s => s);
                    
                // Process the data in batches to avoid memory issues
                int batchSize = 500;
                for (int i = 0; i < response.Data.Count; i += batchSize)
                {
                    // Check for cancellation
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    var batch = response.Data.Skip(i).Take(batchSize).ToList();
                    
                    foreach (var routeStop in batch)
                    {
                        try
                        {
                            string routeKey = GetRouteKeyFromParts(routeStop.Route, routeStop.Bound, routeStop.ServiceType, TransportOperator.KMB);
                            
                            if (routeDictionary.TryGetValue(routeKey, out var route))
                            {
                                // Create a unique ID for the route-stop relationship
                                string relationId = Guid.NewGuid().ToString();
                                
                                try
                                {
                                    // Safely execute database operations with retry logic
                                    var db = _databaseService.GetDatabase();
                                    
                                    try
                                    {
                                        db.Execute("INSERT OR REPLACE INTO RouteStopRelations (Id, RouteId, StopId, Sequence, Direction) VALUES (?, ?, ?, ?, ?)",
                                            relationId, routeKey, routeStop.Stop, routeStop.Seq, routeStop.Bound);
                                            
                                        // Also update legacy table for backward compatibility
                                        db.Execute("INSERT OR REPLACE INTO RouteStops (Id, RouteId, StopId, Sequence) VALUES (?, ?, ?, ?)",
                                            relationId, routeKey, routeStop.Stop, routeStop.Seq);
                                    }
                                    catch (SQLiteException ex) when (ex.Message.Contains("malformed") || ex.Message.Contains("corrupt"))
                                    {
                                        // Handle database corruption
                                        _logger?.LogError(ex, "Database corruption detected while processing route-stop relations");
                                        
                                        if (databaseService != null)
                                        {
                                            bool repaired = databaseService.CheckAndRepairDatabase();
                                            if (repaired)
                                            {
                                                // Retry the operation once if repair was successful
                                                db = _databaseService.GetDatabase();
                                                db.Execute("INSERT OR REPLACE INTO RouteStopRelations (Id, RouteId, StopId, Sequence, Direction) VALUES (?, ?, ?, ?, ?)",
                                                    relationId, routeKey, routeStop.Stop, routeStop.Seq, routeStop.Bound);
                                                    
                                                db.Execute("INSERT OR REPLACE INTO RouteStops (Id, RouteId, StopId, Sequence) VALUES (?, ?, ?, ?)",
                                                    relationId, routeKey, routeStop.Stop, routeStop.Seq);
                                            }
                                            else
                                            {
                                                _logger?.LogError("Unable to repair database after corruption");
                                                
                                                // Notify the exception handler about this issue
                                                if (exceptionHandler != null)
                                                {
                                                    exceptionHandler.HandleUnhandledException(ex, false);
                                                }
                                                
                                                // Skip this batch to avoid further errors
                                                break;
                                            }
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _logger?.LogError(ex, "Error processing route-stop relations for route {routeId}", routeKey);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogError(ex, "Error processing route-stop data");
                        }
                    }
                }
                
                _logger?.LogInformation("Processed {count} routes with route-stops", routeDictionary.Count);
            }
            catch (HttpRequestException ex)
            {
                _logger?.LogError(ex, "Failed to download route-stops data from API");
            }
            catch (OperationCanceledException)
            {
                _logger?.LogInformation("Route-stops download cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in route-stops download");
            }
        }

        /// <summary>
        /// Creates a standardized route key from route parts
        /// </summary>
        private string GetRouteKeyFromParts(string routeNumber, string bound, string serviceType, TransportOperator transportOperator)
        {
            // Normalize service type - default to "1" if empty
            serviceType = string.IsNullOrEmpty(serviceType) ? "1" : serviceType;
            
            // Create a unique key for the route
            return $"{routeNumber}|{serviceType}|{bound}|{(int)transportOperator}";
        }

        /// <summary>
        /// Cleans up old data that wasn't updated during the current download operation
        /// </summary>
        private async Task CleanUpOldDataAsync(DateTime currentTimestamp, CancellationToken cancellationToken)
        {
            // Calculate cutoff time as 1 day before the current operation
            DateTime cutoffTime = currentTimestamp.Subtract(DATA_CLEANUP_AGE);
            _logger.LogInformation("{Prefix} Cleaning up {Operator} data older than {CutoffTime}", 
                LOG_PREFIX, LOG_TRANSPORT_TYPE, cutoffTime);
            
            try
            {
                var db = _databaseService.GetDatabase();
                TransportOperator currentOperator = TransportOperator.KMB; // Current operator being downloaded
                
                // We'll use separate transactions for each table to avoid long-running transactions
                
                // 1. Delete old routes
                _logger.LogDebug("{Prefix} Finding old {Operator} routes to delete", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                await Task.Run(() => 
                {
                    try
                    {
                        db.BeginTransaction();
                        // Get count of records to be deleted for logging
                        var routesCount = db.ExecuteScalar<int>(
                            "SELECT COUNT(*) FROM TransportRoute WHERE Operator = ? AND LastUpdated < ?", 
                            (int)currentOperator, cutoffTime);
                            
                        if (routesCount > 0)
                        {
                            _logger.LogInformation("{Prefix} Deleting {Count} outdated {Operator} routes", 
                                LOG_PREFIX, routesCount, LOG_TRANSPORT_TYPE);
                            db.Execute(
                                "DELETE FROM TransportRoute WHERE Operator = ? AND LastUpdated < ?", 
                                (int)currentOperator, cutoffTime);
                        }
                        db.Commit();
                        _logger.LogDebug("{Prefix} Deleted {Count} outdated {Operator} routes", 
                            LOG_PREFIX, routesCount, LOG_TRANSPORT_TYPE);
                    }
                    catch (Exception ex)
                    {
                        db.Rollback();
                        _logger.LogError(ex, "{Prefix} Error deleting old {Operator} routes", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                        throw;
                    }
                }, cancellationToken);
                
                // 2. Delete old stops that are only used by this operator
                // We need to be careful here - only delete stops that aren't used by other operators
                _logger.LogDebug("{Prefix} Finding old {Operator} stops to delete", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                await Task.Run(() => 
                {
                    try
                    {
                        db.BeginTransaction();
                        // First, identify stops that are exclusively used by current operator and are outdated
                        var stopsCount = db.ExecuteScalar<int>(
                            "SELECT COUNT(*) FROM TransportStop " +
                            "WHERE Operator = ? AND LastUpdated < ? " +
                            "AND NOT EXISTS (SELECT 1 FROM RouteStopRelations rs " +
                            "JOIN TransportRoute r ON rs.RouteId = r.Id " +
                            "WHERE rs.StopId = TransportStop.Id AND r.Operator != ?)",
                            (int)currentOperator, cutoffTime, (int)currentOperator);
                        
                        if (stopsCount > 0)
                        {
                            _logger.LogInformation("{Prefix} Deleting {Count} outdated {Operator} stops", 
                                LOG_PREFIX, stopsCount, LOG_TRANSPORT_TYPE);
                            db.Execute(
                                "DELETE FROM TransportStop " +
                                "WHERE Operator = ? AND LastUpdated < ? " +
                                "AND NOT EXISTS (SELECT 1 FROM RouteStopRelations rs " +
                                "JOIN TransportRoute r ON rs.RouteId = r.Id " +
                                "WHERE rs.StopId = TransportStop.Id AND r.Operator != ?)",
                                (int)currentOperator, cutoffTime, (int)currentOperator);
                        }
                        db.Commit();
                        _logger.LogDebug("{Prefix} Deleted {Count} outdated {Operator} stops", 
                            LOG_PREFIX, stopsCount, LOG_TRANSPORT_TYPE);
                    }
                    catch (Exception ex)
                    {
                        db.Rollback();
                        _logger.LogError(ex, "{Prefix} Error deleting old {Operator} stops", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                        throw;
                    }
                }, cancellationToken);
                
                // 3. Delete old route-stop relations
                _logger.LogDebug("{Prefix} Finding old {Operator} route-stop relations to delete", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                await Task.Run(() => 
                {
                    try
                    {
                        db.BeginTransaction();
                        // Delete relations associated with current operator routes that are outdated
                        var relationsCount = db.ExecuteScalar<int>(
                            "SELECT COUNT(*) FROM RouteStopRelations rs " +
                            "JOIN TransportRoute r ON rs.RouteId = r.Id " +
                            "WHERE r.Operator = ? AND rs.LastUpdated < ?",
                            (int)currentOperator, cutoffTime);
                        
                        if (relationsCount > 0)
                        {
                            _logger.LogInformation("{Prefix} Deleting {Count} outdated {Operator} route-stop relations", 
                                LOG_PREFIX, relationsCount, LOG_TRANSPORT_TYPE);
                            db.Execute(
                                "DELETE FROM RouteStopRelations " +
                                "WHERE RouteId IN (SELECT Id FROM TransportRoute WHERE Operator = ?) " +
                                "AND LastUpdated < ?",
                                (int)currentOperator, cutoffTime);
                        }
                        db.Commit();
                        _logger.LogDebug("{Prefix} Deleted {Count} outdated {Operator} route-stop relations", 
                            LOG_PREFIX, relationsCount, LOG_TRANSPORT_TYPE);
                    }
                    catch (Exception ex)
                    {
                        db.Rollback();
                        _logger.LogError(ex, "{Prefix} Error deleting old {Operator} route-stop relations", 
                            LOG_PREFIX, LOG_TRANSPORT_TYPE);
                        throw;
                    }
                }, cancellationToken);
                
                // Clear route stops cache to ensure we fetch fresh data next time
                _databaseService.ClearRouteStopsCache();
                
                _logger.LogInformation("{Prefix} {Operator} data cleanup completed successfully", LOG_PREFIX, LOG_TRANSPORT_TYPE);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Prefix} Failed to clean up old {Operator} data", LOG_PREFIX, LOG_TRANSPORT_TYPE);
                throw;
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