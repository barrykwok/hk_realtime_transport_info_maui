using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using hk_realtime_transport_info_maui.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Maui.Controls;

namespace hk_realtime_transport_info_maui.Services
{
    public class MtrDataService
    {
        private readonly ILogger<MtrDataService> _logger;
        private readonly HttpClientUtility _httpClient;
        private readonly LiteDbService _dbService;
        private readonly CacheService _cacheService;
        
        // Constants for data sources
        private const string MtrBusRoutesUrl = "https://opendata.mtr.com.hk/data/mtr_bus_routes.csv";
        private const string MtrBusStopsUrl = "https://opendata.mtr.com.hk/data/mtr_bus_stops.csv";
        private const string MtrEtaApiBaseUrl = "https://rt.data.gov.hk/v1/transport/mtr";
        private const string MtrBusApiUrl = "https://rt.data.gov.hk/v1/transport/mtr/bus/getSchedule";
        
        // Cache validity period (in seconds)
        private const int CacheValidityPeriod = 30;
        
        // Constants for standardized logging
        private const string LOG_PREFIX = "[MtrDataService]";
        
        // Data freshness settings
        private static readonly TimeSpan DATA_EXPIRATION = TimeSpan.FromDays(7);
        
        // Progress reporting
        public event EventHandler<DownloadProgressEventArgs>? ProgressChanged;
        
        // Background processing control
        private readonly CancellationTokenSource _downloadCts = new CancellationTokenSource();
        private Task? _currentDownloadTask;
        private bool _isDownloading;
        
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
        
        public MtrDataService(LiteDbService dbService, HttpClientUtility httpClient, ILogger<MtrDataService> logger, CacheService cacheService)
        {
            _logger = logger;
            _httpClient = httpClient;
            _dbService = dbService;
            _cacheService = cacheService;
        }
        
        protected virtual void OnProgressChanged(double progress, string statusMessage, bool dataWasUpdated = true)
        {
            // Always dispatch progress events to the main thread
            MainThread.BeginInvokeOnMainThread(() => {
                ProgressChanged?.Invoke(this, new DownloadProgressEventArgs(progress, statusMessage, dataWasUpdated));
            });
        }
        
        private void ReportProgress(IProgress<(int, int, string)>? progress, int current, int? total, string message)
        {
            progress?.Report((current, total ?? 0, message));
            
            // Also report through event if available
            if (total.HasValue)
            {
                double progressValue = total.Value > 0 ? (double)current / total.Value : 0;
                OnProgressChanged(progressValue, message);
            }
            else
            {
                OnProgressChanged(0, message);
            }
        }
        
        /// <summary>
        /// Starts the data download process for MTR data
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
            
            // Check data freshness to determine if download is needed
            if (!forceRefresh)
            {
                var freshness = CheckMtrDataFreshness();
                
                // Download is needed if either routes or stops are missing or not fresh
                downloadNeeded = !freshness.routesExist || !freshness.routesFresh || 
                                 !freshness.stopsExist || !freshness.stopsFresh;
                
                _logger?.LogInformation("{0} Data freshness check: Routes={1} (Fresh={2}, Age={3:F1} days), " +
                                       "Stops={4} (Fresh={5}, Age={6:F1} days), Download needed={7}", 
                    LOG_PREFIX, 
                    freshness.routesExist, freshness.routesFresh, freshness.routesAge?.TotalDays,
                    freshness.stopsExist, freshness.stopsFresh, freshness.stopsAge?.TotalDays,
                    downloadNeeded);
                
                if (!downloadNeeded)
                {
                    _logger?.LogInformation("{0} All MTR data is fresh, skipping download", LOG_PREFIX);
                    OnProgressChanged(1.0, "MTR data is up-to-date", false);
                    return Task.CompletedTask;
                }
            }
            
            // Begin download process
            _isDownloading = true;
            _currentDownloadTask = DownloadAllDataInternalAsync(_downloadCts.Token, forceRefresh, downloadNeeded);
            
            return _currentDownloadTask;
        }
        
        /// <summary>
        /// Checks if data exists for MTR and whether it's fresh or expired
        /// </summary>
        private (bool dataExists, bool dataIsFresh) CheckDataFreshness(TransportOperator operatorType)
        {
            try
            {
                // Get all records from database
                var routes = _dbService.GetAllRecords<TransportRoute>("TransportRoutes")
                    .Where(r => r.Operator == operatorType)
                    .ToList();
                
                bool dataExists = routes.Count > 0;
                
                if (!dataExists)
                {
                    _logger?.LogInformation("{0} No data found for operator {1}", LOG_PREFIX, operatorType);
                    return (false, false);
                }
                
                // Check data freshness (using the newest route's timestamp)
                var newestRoute = routes.OrderByDescending(r => r.LastUpdated).FirstOrDefault();
                if (newestRoute == null)
                {
                    _logger?.LogWarning("{0} No routes found for {1} despite previous check", LOG_PREFIX, operatorType);
                    return (false, false);
                }
                
                var dataAge = DateTime.UtcNow - newestRoute.LastUpdated;
                bool dataIsFresh = dataAge <= DATA_EXPIRATION;
                
                _logger?.LogInformation("{0} Data for {1}: Age={2:F1} days, Fresh={3}", 
                    LOG_PREFIX, operatorType, dataAge.TotalDays, dataIsFresh);
                
                return (dataExists, dataIsFresh);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "{0} Error checking data freshness for {1}", LOG_PREFIX, operatorType);
                
                // In case of database error, assume data exists and is fresh
                // This prevents unnecessary downloads when database access fails temporarily
                _logger?.LogWarning("{0} Database error occurred. Assuming data exists and is fresh to prevent unnecessary download", LOG_PREFIX);
                
                return (true, true);
            }
        }
        
        /// <summary>
        /// Publicly accessible method to check if MTR data is fresh without initiating downloads
        /// </summary>
        /// <returns>A tuple with detailed freshness information</returns>
        public (bool routesExist, bool routesFresh, bool stopsExist, bool stopsFresh, TimeSpan? routesAge, TimeSpan? stopsAge) CheckMtrDataFreshness()
        {
            _logger?.LogInformation("{0} Checking MTR data freshness", LOG_PREFIX);
            
            bool routesExist = false;
            bool routesFresh = false;
            bool stopsExist = false;
            bool stopsFresh = false;
            TimeSpan? routesAge = null;
            TimeSpan? stopsAge = null;
            
            try
            {
                // Check routes freshness
                var routes = _dbService.GetAllRecords<TransportRoute>("TransportRoutes")
                    .Where(r => r.Operator == TransportOperator.MTR)
                    .ToList();
                
                routesExist = routes.Count > 0;
                
                if (routesExist)
                {
                    var newestRoute = routes.OrderByDescending(r => r.LastUpdated).FirstOrDefault();
                    if (newestRoute != null)
                    {
                        routesAge = DateTime.UtcNow - newestRoute.LastUpdated;
                        routesFresh = routesAge <= DATA_EXPIRATION;
                        
                        _logger?.LogDebug("{0} Routes data: Count={1}, Age={2:F1} days, Fresh={3}", 
                            LOG_PREFIX, routes.Count, routesAge?.TotalDays, routesFresh);
                    }
                }
                
                // Check stops freshness
                var stops = _dbService.GetAllRecords<TransportStop>("TransportStops")
                    .Where(s => s.Operator == TransportOperator.MTR)
                    .ToList();
                
                stopsExist = stops.Count > 0;
                
                if (stopsExist)
                {
                    // Use cache timestamp for stops if available
                    var stopsCacheTimestamp = _cacheService.Get<DateTime?>("MTR_Stops_LastUpdate");
                    
                    if (stopsCacheTimestamp.HasValue)
                    {
                        stopsAge = DateTime.UtcNow - stopsCacheTimestamp.Value;
                        stopsFresh = stopsAge <= DATA_EXPIRATION;
                    }
                    else
                    {
                        // Fall back to checking individual stop records
                        var newestStop = stops
                            .OrderByDescending(s => s.LastUpdated)
                            .FirstOrDefault();
                        
                        if (newestStop != null)
                        {
                            stopsAge = DateTime.UtcNow - newestStop.LastUpdated;
                            stopsFresh = stopsAge <= DATA_EXPIRATION;
                        }
                    }
                    
                    _logger?.LogDebug("{0} Stops data: Count={1}, Age={2:F1} days, Fresh={3}", 
                        LOG_PREFIX, stops.Count, stopsAge?.TotalDays, stopsFresh);
                }
                
                var overallFresh = (routesExist && routesFresh) && (stopsExist && stopsFresh);
                _logger?.LogInformation("{0} MTR data freshness: Routes={1} (Fresh={2}), Stops={3} (Fresh={4}), Overall Fresh={5}",
                    LOG_PREFIX, routesExist, routesFresh, stopsExist, stopsFresh, overallFresh);
                
                return (routesExist, routesFresh, stopsExist, stopsFresh, routesAge, stopsAge);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "{0} Error checking MTR data freshness", LOG_PREFIX);
                return (false, false, false, false, null, null);
            }
        }
        
        private async Task DownloadAllDataInternalAsync(CancellationToken cancellationToken, bool forceRefresh = false, bool downloadNeeded = true)
        {
            try
            {
                _logger?.LogInformation("{0} Starting MTR data download (Force: {1}, Needed: {2})", 
                    LOG_PREFIX, forceRefresh, downloadNeeded);
                
                OnProgressChanged(0.0, "Starting MTR data download...");
                
                // Track current timestamp for cleanup operations
                var currentTimestamp = DateTime.UtcNow;
                
                // Download routes first (30% progress)
                if (downloadNeeded)
                {
                    _logger?.LogInformation("{0} Downloading MTR routes", LOG_PREFIX);
                    OnProgressChanged(0.1, "Downloading MTR routes..."); // Adjusted to 0.1 start
                    
                    try
                    {
                        var routeProgress = new Progress<(int, int, string)>(p => {
                            double scaledProgress = 0.1 + (p.Item1 / (double)(p.Item2 > 0 ? p.Item2 : 1)) * 0.3; // Scale 0.1 to 0.4
                            OnProgressChanged(scaledProgress, p.Item3);
                        });
                        
                        await DownloadRoutesAsync(routeProgress, forceRefresh);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "{0} Error downloading MTR routes", LOG_PREFIX);
                        OnProgressChanged(0.4, $"Error downloading MTR routes: {ex.Message}"); // End of route download section
                    }
                }
                
                // Download stops and process route-stop relations (60% progress)
                if (downloadNeeded && !cancellationToken.IsCancellationRequested)
                {
                    _logger?.LogInformation("{0} Downloading MTR stops and processing relations", LOG_PREFIX);
                    OnProgressChanged(0.4, "Downloading MTR stops & relations..."); // Start of stops/relations section
                    
                    try
                    {
                        var stopsProgress = new Progress<(int, int, string)>(p => {
                            double scaledProgress = 0.4 + (p.Item1 / (double)(p.Item2 > 0 ? p.Item2 : 1)) * 0.5; // Scale 0.4 to 0.9
                            OnProgressChanged(scaledProgress, p.Item3);
                        });
                        
                        await DownloadStopsAsync(stopsProgress, forceRefresh); // This will now also handle route-stop relations
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "{0} Error downloading MTR stops/relations", LOG_PREFIX);
                        OnProgressChanged(0.9, $"Error downloading MTR stops/relations: {ex.Message}"); // End of stops/relations section
                    }
                }
                
                // Clean up old data if needed (10% progress)
                if (downloadNeeded && !cancellationToken.IsCancellationRequested)
                {
                    _logger?.LogInformation("{0} Performing cleanup (if any)", LOG_PREFIX);
                    OnProgressChanged(0.9, "Performing cleanup...");

                    try
                    {
                        // await CleanUpOldDataAsync(currentTimestamp, cancellationToken); // Placeholder for cleanup
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "{0} Error cleaning up old data", LOG_PREFIX);
                    }
                }
                
                _logger?.LogInformation("{0} MTR data download completed successfully", LOG_PREFIX);
                OnProgressChanged(1.0, "MTR data download completed");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "{0} Error downloading MTR data", LOG_PREFIX);
                OnProgressChanged(1.0, $"Error downloading MTR data: {ex.Message}");
            }
            finally
            {
                _isDownloading = false;
            }
        }
        
        public async Task<List<TransportRoute>> DownloadRoutesAsync(IProgress<(int, int, string)>? progress = null, bool forceUpdate = false)
        {
            _logger.LogInformation("{0} Starting MTR routes download (forceUpdate: {1})", LOG_PREFIX, forceUpdate);
            ReportProgress(progress, 0, null, "Starting MTR routes download");
            
            if (!forceUpdate)
            {
                // Check if data is fresh enough
                var lastUpdate = _cacheService.Get<DateTime?>("MTR_Routes_LastUpdate");
                if (lastUpdate.HasValue)
                {
                    var dataAge = DateTime.Now - lastUpdate.Value;
                    var isFresh = dataAge < DATA_EXPIRATION;
                    
                    _logger.LogDebug("{0} MTR Routes: Last update: {1}, Age: {2} hours, Sync interval: {3} days, Fresh: {4}", 
                        LOG_PREFIX, lastUpdate, dataAge.TotalHours, DATA_EXPIRATION.TotalDays, isFresh);
                    
                    if (isFresh)
                    {
                        _logger.LogInformation("{0} MTR routes data is fresh, skipping download.", LOG_PREFIX);
                        ReportProgress(progress, 1, 1, "MTR routes data is up-to-date.");
                        
                        // Return existing routes
                        return _dbService.GetAllRecords<TransportRoute>("TransportRoutes")
                            .Where(r => r.Operator == TransportOperator.MTR)
                            .ToList();
                    }
                }
                else
                {
                    _logger.LogDebug("{0} MTR Routes: No last update timestamp found. Will download.", LOG_PREFIX);
                }
            }
            
            try
            {
                // Download the CSV file
                var response = await _httpClient.GetAsync(MtrBusRoutesUrl);
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("{0} MTR Routes download failed, status={1}", LOG_PREFIX, response.StatusCode);
                    throw new Exception($"Failed to download MTR routes: {response.StatusCode}");
                }
                
                _logger.LogInformation("{0} MTR Routes data received, status={1}", LOG_PREFIX, response.StatusCode);
                
                // Get content as string
                var csvContent = await response.Content.ReadAsStringAsync();
                
                // Log sample of raw CSV data for debugging
                _logger.LogDebug("{0} Sample of raw routes CSV:\n{1}", LOG_PREFIX, GetSampleCsvContent(csvContent, 5));
                
                // Parse the CSV data manually since we don't have CsvHelper dependency
                var routesData = ParseCsvData(csvContent);
                
                _logger.LogInformation("{0} Processing {1} routes", LOG_PREFIX, routesData.Count);
                
                // Get existing route keys to prevent duplicates
                var existingRoutes = _dbService.GetAllRecords<TransportRoute>("TransportRoutes")
                    .Where(r => r.Operator == TransportOperator.MTR)
                    .ToList();
                var existingKeys = existingRoutes.Select(r => r.Key).ToHashSet();
                
                // Create a list to track unique routes
                var uniqueRoutes = new List<TransportRoute>();
                
                foreach (var routeData in routesData.Skip(1)) // Skip header row
                {
                    // Skip if row doesn't have enough columns
                    if (routeData.Length < 4) continue;
                    
                    var route = routeData[0];
                    var chn = routeData[1];
                    var eng = routeData[2];
                    
                    if (string.IsNullOrEmpty(route)) continue;
                    
                    // Parse Chinese and English parts
                    string[] zhParts = null;
                    string[] enParts = null;
                    
                    try
                    {
                        if (chn.Contains("至"))
                        {
                            zhParts = chn.Split('至');
                        }
                        else
                        {
                            continue;
                        }
                        
                        if (eng.Contains(" to "))
                        {
                            enParts = eng.Split(" to ");
                        }
                        else
                        {
                            continue;
                        }
                        
                        if (zhParts.Length != 2 || enParts.Length != 2) continue;
                    }
                    catch
                    {
                        continue;
                    }
                    
                    var startZh = zhParts[0].Trim();
                    var endZh = zhParts[1].Trim();
                    var startEn = enParts[0].Trim();
                    var endEn = enParts[1].Trim();
                    
                    // Create inbound and outbound routes
                    foreach (var bound in new[] { "I", "O" })
                    {
                        // Generate a unique route key
                        var routeKey = $"MTR_{route}_{1}_{bound}";
                        
                        // Skip if this key already exists in database
                        if (existingKeys.Contains(routeKey))
                        {
                            continue;
                        }
                        
                        var busRoute = new TransportRoute
                        {
                            Id = routeKey,
                            RouteNumber = route,
                            Operator = TransportOperator.MTR,
                            Type = TransportType.MTR,
                            ServiceType = "1",
                            Bound = bound,
                            OriginEn = bound == "O" ? startEn : endEn,
                            DestinationEn = bound == "O" ? endEn : startEn,
                            OriginZhHant = bound == "O" ? startZh : endZh,
                            DestinationZhHant = bound == "O" ? endZh : startZh,
                            // Use Traditional Chinese for simplified Chinese fields too
                            // as MTR doesn't provide separate SC
                            OriginZhHans = bound == "O" ? startZh : endZh,
                            DestinationZhHans = bound == "O" ? endZh : startZh,
                            LastUpdated = DateTime.UtcNow
                        };
                        
                        // Only add if this key doesn't already exist
                        if (!uniqueRoutes.Any(r => r.Key == routeKey))
                        {
                            uniqueRoutes.Add(busRoute);
                        }
                    }
                }
                
                _logger.LogInformation("{0} Found {1} new routes to add", LOG_PREFIX, uniqueRoutes.Count);
                
                if (uniqueRoutes.Count > 0)
                {
                    _logger.LogInformation("{0} Saving {1} routes to database", LOG_PREFIX, uniqueRoutes.Count);
                    _dbService.BulkInsert("TransportRoutes", uniqueRoutes);
                }
                else
                {
                    _logger.LogInformation("{0} No new routes to insert", LOG_PREFIX);
                }
                
                // Update the timestamp
                _cacheService.Set("MTR_Routes_LastUpdate", DateTime.Now, DATA_EXPIRATION);
                _logger.LogDebug("{0} MTR Routes timestamp updated to {1}", LOG_PREFIX, DateTime.Now);
                
                // Return all routes, including existing ones
                var allRoutes = _dbService.GetAllRecords<TransportRoute>("TransportRoutes")
                    .Where(r => r.Operator == TransportOperator.MTR)
                    .ToList();
                _logger.LogInformation("{0} Route download completed successfully, total {1} routes available", LOG_PREFIX, allRoutes.Count);
                
                ReportProgress(progress, 1, 1, $"Downloaded {uniqueRoutes.Count} MTR routes.");
                return allRoutes;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{0} MTR Routes download failed", LOG_PREFIX);
                ReportProgress(progress, 0, 1, $"Failed to download MTR routes: {ex.Message}");
                return new List<TransportRoute>();
            }
        }
        
        public async Task<List<TransportStop>> DownloadStopsAsync(IProgress<(int, int, string)>? progress = null, bool forceUpdate = false)
        {
            _logger.LogInformation("{0} Starting MTR stops download (forceUpdate: {1})", LOG_PREFIX, forceUpdate);
            ReportProgress(progress, 0, null, "Starting MTR stops download");
            
            if (!forceUpdate)
            {
                var lastStopsUpdate = _cacheService.Get<DateTime?>("MTR_Stops_LastUpdate");
                var lastRelationsUpdate = _cacheService.Get<DateTime?>("MTR_RouteStops_LastUpdate");

                if (lastStopsUpdate.HasValue && lastRelationsUpdate.HasValue)
                {
                    var stopsAge = DateTime.Now - lastStopsUpdate.Value;
                    var relationsAge = DateTime.Now - lastRelationsUpdate.Value;
                    var isFresh = stopsAge < DATA_EXPIRATION && relationsAge < DATA_EXPIRATION;

                    _logger.LogDebug("{0} MTR Stops: Last update: {1}, Age: {2} hours. Relations Last Update: {3}, Age: {4} hours. Interval: {5} days, Fresh: {6}",
                        LOG_PREFIX, lastStopsUpdate, stopsAge.TotalHours, lastRelationsUpdate, relationsAge.TotalHours, DATA_EXPIRATION.TotalDays, isFresh);

                    if (isFresh)
                    {
                        _logger.LogInformation("{0} MTR stops and relations data is fresh, skipping download.", LOG_PREFIX);
                        ReportProgress(progress, 1, 1, "MTR stops and relations data is up-to-date.");
                        return _dbService.GetAllRecords<TransportStop>("TransportStops")
                            .Where(s => s.Operator == TransportOperator.MTR)
                            .ToList();
                    }
                }
                else
                {
                    _logger.LogDebug("{0} MTR Stops or Relations: No last update timestamp found for one or both. Will download.", LOG_PREFIX);
                }
            }

            try
            {
                var response = await _httpClient.GetAsync(MtrBusStopsUrl);
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError("{0} MTR Stops download failed, status={1}", LOG_PREFIX, response.StatusCode);
                    throw new Exception($"Failed to download MTR stops: {response.StatusCode}");
                }
                _logger.LogInformation("{0} MTR Stops data received, status={1}", LOG_PREFIX, response.StatusCode);
                var csvContent = await response.Content.ReadAsStringAsync();
                _logger.LogDebug("{0} Sample of raw stops CSV:\n{1}", LOG_PREFIX, GetSampleCsvContent(csvContent, 5));

                var stopsData = ParseCsvData(csvContent);
                _logger.LogInformation("{0} Processing {1} stops and their relations", LOG_PREFIX, stopsData.Count);

                var existingStops = _dbService.GetAllRecords<TransportStop>("TransportStops")
                    .Where(r => r.Operator == TransportOperator.MTR)
                    .ToList();
                var existingStopKeys = existingStops.Select(s => s.Id).ToHashSet();
                
                var existingRelations = _dbService.GetAllRecords<RouteStopRelation>("RouteStopRelations")
                                     .Where(rs => rs.RouteId.StartsWith("MTR-")) // Filter MTR relations
                                     .ToList();
                var existingRelationKeys = existingRelations.Select(rs => $"{rs.RouteId}_{rs.StopId}_{rs.Direction}_{rs.Sequence}").ToHashSet();

                var uniqueStops = new List<TransportStop>();
                var uniqueRelations = new List<RouteStopRelation>();

                foreach (var stopRowData in stopsData.Skip(1)) // Skip header row
                {
                    if (stopRowData.Length < 8) continue; // Ensure enough columns

                    try
                    {
                        var routeIdCsv = stopRowData[0];
                        var direction = stopRowData[1];
                        var stationSeqnoStr = stopRowData[2];
                        var stationIdCsv = stopRowData[3];
                        var latString = stopRowData[4];
                        var lngString = stopRowData[5];
                        var nameZh = stopRowData[6];
                        var nameEn = stopRowData[7];

                        if (string.IsNullOrEmpty(stationIdCsv) || string.IsNullOrEmpty(routeIdCsv)) continue;

                        double lat = 0.0, lng = 0.0;
                        try
                        {
                            lat = double.Parse(latString, CultureInfo.InvariantCulture);
                            lng = double.Parse(lngString, CultureInfo.InvariantCulture);
                            if (lat < -90 || lat > 90) { _logger.LogWarning("Invalid latitude {0} for stop {1}", lat, stationIdCsv); lat = 0.0; }
                            if (lng < -180 || lng > 180) { _logger.LogWarning("Invalid longitude {0} for stop {1}", lng, stationIdCsv); lng = 0.0; }
                        }
                        catch
                        {
                            _logger.LogWarning("Failed to parse coordinates for stop {0}: LatString='{1}', LngString='{2}', using defaults.", stationIdCsv, latString, lngString);
                        }

                        var stopKey = $"MTR-{stationIdCsv}";
                        if (!existingStopKeys.Contains(stopKey) && !uniqueStops.Any(s => s.Id == stopKey))
                        {
                            uniqueStops.Add(new TransportStop
                            {
                                Id = stopKey,
                                StopId = stationIdCsv,
                                NameEn = nameEn,
                                NameZh = nameZh,
                                NameZhHant = nameZh,
                                NameZhHans = nameZh,
                                Latitude = lat,
                                Longitude = lng,
                                Operator = TransportOperator.MTR
                            });
                        }
                        
                        // Process RouteStopRelation
                        if (int.TryParse(stationSeqnoStr, out int stopSequence))
                        {
                            // Construct RouteId to match TransportRoute.Key format from DownloadRoutesAsync
                            // TransportRoute.Key is like: $"MTR_{route}_{serviceType}_{bound}"
                            // Here, routeIdCsv is {route}, serviceType is hardcoded to "1" for MTR in DownloadRoutesAsync,
                            // and direction is {bound}.
                            var relationRouteId = $"MTR_{routeIdCsv}_1_{direction}"; 
                            var relationStopId = stopKey; // Already in correct format: MTR-{STATION_ID}
                            var relationKey = $"{relationRouteId}_{relationStopId}_{direction}_{stopSequence}";

                            if (!existingRelationKeys.Contains(relationKey) && !uniqueRelations.Any(r => $"{r.RouteId}_{r.StopId}_{r.Direction}_{r.Sequence}" == relationKey))
                            {
                                uniqueRelations.Add(new RouteStopRelation
                                {
                                    RouteId = relationRouteId,
                                    StopId = relationStopId,
                                    Direction = direction,
                                    Sequence = stopSequence,
                                    Operator = TransportOperator.MTR
                                });
                            }
                        }
                        else
                        {
                            _logger.LogWarning("{0} Invalid StopSequence '{1}' for RouteID '{2}', StopID '{3}'", LOG_PREFIX, stationSeqnoStr, routeIdCsv, stationIdCsv);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "{0} Error processing stop/relation data row: {1}", LOG_PREFIX, string.Join(",", stopRowData));
                    }
                }

                _logger.LogInformation("{0} Found {1} new stops and {2} new relations to add", LOG_PREFIX, uniqueStops.Count, uniqueRelations.Count);

                if (uniqueStops.Any())
                {
                    _logger.LogInformation("{0} Saving {1} stops to database", LOG_PREFIX, uniqueStops.Count);
                    _dbService.BulkInsert("TransportStops", uniqueStops);
                }
                _cacheService.Set("MTR_Stops_LastUpdate", DateTime.Now, DATA_EXPIRATION);
                _logger.LogDebug("{0} MTR Stops timestamp updated to {1}", LOG_PREFIX, DateTime.Now);
                
                if (uniqueRelations.Any())
                {
                    _logger.LogInformation("{0} Saving {1} route-stop relations to database", LOG_PREFIX, uniqueRelations.Count);
                    _dbService.BulkInsert("RouteStopRelations", uniqueRelations);
                }
                _cacheService.Set("MTR_RouteStops_LastUpdate", DateTime.Now, DATA_EXPIRATION);
                _logger.LogDebug("{0} MTR RouteStops timestamp updated to {1}", LOG_PREFIX, DateTime.Now);

                var allStops = _dbService.GetAllRecords<TransportStop>("TransportStops")
                    .Where(s => s.Operator == TransportOperator.MTR)
                    .ToList();
                _logger.LogInformation("{0} Stops & relations processing completed. Total {1} MTR stops, {2} new relations.", LOG_PREFIX, allStops.Count, uniqueRelations.Count);
                
                ReportProgress(progress, 1, 1, $"Processed {uniqueStops.Count} MTR stops & {uniqueRelations.Count} relations.");
                return allStops;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{0} MTR Stops download failed", LOG_PREFIX);
                ReportProgress(progress, 0, 1, $"Failed to download MTR stops: {ex.Message}");
                return new List<TransportStop>();
            }
        }
        
        /// <summary>
        /// Public method to cancel any ongoing downloads
        /// </summary>
        public void CancelDownloads()
        {
            _logger?.LogInformation("{0} Cancelling downloads", LOG_PREFIX);
            try
            {
                _downloadCts.Cancel();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "{0} Error cancelling downloads", LOG_PREFIX);
            }
        }
        
        /// <summary>
        /// Cleans up resources when the service is disposed
        /// </summary>
        public void Dispose()
        {
            CancelDownloads();
            _downloadCts.Dispose();
        }
        
        // Helper utility methods
        
        // Get a sample of CSV content (first N lines)
        private string GetSampleCsvContent(string csv, int lines)
        {
            var allLines = csv.Split('\n');
            var sampleLines = allLines.Take(lines);
            return string.Join('\n', sampleLines);
        }
        
        // Parse CSV data manually without CsvHelper
        private List<string[]> ParseCsvData(string csvContent)
        {
            var result = new List<string[]>();
            var lines = csvContent.Split('\n');
            
            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                
                // Basic CSV parsing - doesn't handle quotes or commas within fields
                var fields = line.Split(',');
                result.Add(fields);
            }
            
            return result;
        }
    }
} 