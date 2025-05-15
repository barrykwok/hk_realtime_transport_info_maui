using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using hk_realtime_transport_info_maui.Models;
using Microsoft.Extensions.Logging;
using NGeoHash;

namespace hk_realtime_transport_info_maui.Services
{
    public class LocationCacheService
    {
        private readonly LiteDbService _dbService;
        private readonly ILogger<LocationCacheService> _logger;
        private const int MaxCachedLocations = 100;
        private const int DefaultSearchRadius = 1000; // 1 km
        private const int GeoHashPrecisionForLocationMatching = 8; // ~38m x 19m precision
        
        public LocationCacheService(LiteDbService dbService, ILogger<LocationCacheService> logger)
        {
            _dbService = dbService;
            _logger = logger;
        }
        
        /// <summary>
        /// Get nearby stops for the given location, using cache if available
        /// </summary>
        /// <param name="latitude">User's latitude</param>
        /// <param name="longitude">User's longitude</param>
        /// <param name="radiusMeters">Search radius in meters</param>
        /// <param name="forceRefresh">If true, ignores cache and fetches fresh data</param>
        /// <returns>List of transport stops near the location</returns>
        public async Task<List<TransportStop>> GetNearbyStopsAsync(double latitude, double longitude, int radiusMeters = DefaultSearchRadius, bool forceRefresh = false)
        {
            if (latitude == 0 && longitude == 0)
            {
                _logger.LogWarning("Invalid coordinates provided to GetNearbyStopsAsync");
                return new List<TransportStop>();
            }
            
            try
            {
                // Check if we have a cached result for this location
                if (!forceRefresh)
                {
                    var cachedLocation = FindCachedLocationMatch(latitude, longitude);
                    if (cachedLocation != null)
                    {
                        _logger.LogDebug("Using cached location data for {Lat}, {Lon} (GeoHash8: {GeoHash})", 
                            latitude, longitude, cachedLocation.GeoHash8);
                        
                        // Update access statistics
                        cachedLocation.LastAccessed = DateTime.UtcNow;
                        cachedLocation.AccessCount++;
                        await UpdateCachedLocationAsync(cachedLocation);
                        
                        // Return stops from cache
                        var cachedStops = await GetStopsFromCachedLocationAsync(cachedLocation);
                        
                        // If no stops were found in the cache, do a direct database query
                        if (cachedStops.Count == 0)
                        {
                            _logger.LogWarning("Cached location had no stops - forcing refresh");
                            forceRefresh = true;
                        }
                        else
                        {
                            return cachedStops;
                        }
                    }
                }
                
                // No cache hit or refresh forced, get fresh data
                _logger.LogDebug("Cache miss for location {Lat}, {Lon}, fetching fresh data", latitude, longitude);
                var nearbyStops = _dbService.FindStopsNearLocation(latitude, longitude, radiusMeters);
                
                _logger.LogInformation("Direct database query found {Count} stops near location {Lat}, {Lon}", 
                    nearbyStops.Count, latitude, longitude);
                
                // Cache the result for future use
                await CacheLocationAsync(latitude, longitude, nearbyStops, radiusMeters);
                
                return nearbyStops;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting nearby stops for location {Lat}, {Lon}", latitude, longitude);
                return new List<TransportStop>();
            }
        }
        
        /// <summary>
        /// Find a cached location that matches the given coordinates
        /// </summary>
        /// <param name="latitude">The latitude to check</param>
        /// <param name="longitude">The longitude to check</param>
        /// <returns>Matching cached location or null if not found</returns>
        private CachedLocation FindCachedLocationMatch(double latitude, double longitude)
        {
            try
            {
                // Generate geohash for quick comparison
                string geoHash8 = GeoHash.Encode(latitude, longitude, 8);
                
                // Get all cached locations
                var cachedLocations = _dbService.GetAllRecords<CachedLocation>(CollectionNames.CACHED_LOCATIONS);
                
                // First try exact match on GeoHash8
                var exactMatch = cachedLocations.FirstOrDefault(c => c.GeoHash8 == geoHash8);
                if (exactMatch != null)
                {
                    return exactMatch;
                }
                
                // Then try GeoHash7 (street level) match
                string geoHash7 = geoHash8.Substring(0, 7);
                var nearbyMatch = cachedLocations.FirstOrDefault(c => c.GeoHash7 == geoHash7);
                if (nearbyMatch != null)
                {
                    return nearbyMatch;
                }
                
                // No match found
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error finding cached location match");
                return null;
            }
        }
        
        /// <summary>
        /// Get stops from a cached location entry
        /// </summary>
        /// <param name="cachedLocation">The cached location entry</param>
        /// <returns>List of nearby transport stops</returns>
        private async Task<List<TransportStop>> GetStopsFromCachedLocationAsync(CachedLocation cachedLocation)
        {
            try
            {
                // Load all stops from their IDs
                var stops = new List<TransportStop>();
                
                foreach (var stopId in cachedLocation.NearbyStopIds)
                {
                    var stop = _dbService.GetRecordById<TransportStop>(CollectionNames.STOPS, stopId);
                    if (stop != null)
                    {
                        // Calculate distance from the cached location
                        stop.DistanceFromUser = CalculateDistanceInMeters(
                            cachedLocation.Latitude, cachedLocation.Longitude,
                            stop.Latitude, stop.Longitude);
                            
                        stops.Add(stop);
                    }
                }
                
                // If no stops were found in the cache, do a direct database query
                if (stops.Count == 0)
                {
                    _logger.LogInformation("Cache returned 0 stops for location {Lat}, {Lon} - falling back to direct database query", 
                        cachedLocation.Latitude, cachedLocation.Longitude);
                    
                    // Force a refresh to search the database directly
                    return _dbService.FindStopsNearLocation(cachedLocation.Latitude, cachedLocation.Longitude, cachedLocation.RadiusMeters);
                }
                
                // Sort by distance
                return stops.OrderBy(s => s.DistanceFromUser).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting stops from cached location");
                return new List<TransportStop>();
            }
        }
        
        /// <summary>
        /// Cache a location with its nearby stops
        /// </summary>
        /// <param name="latitude">The latitude</param>
        /// <param name="longitude">The longitude</param>
        /// <param name="nearbyStops">The nearby stops</param>
        /// <param name="radiusMeters">Search radius in meters</param>
        private async Task CacheLocationAsync(double latitude, double longitude, List<TransportStop> nearbyStops, int radiusMeters)
        {
            try
            {
                // Create new cached location
                var cachedLocation = new CachedLocation
                {
                    Latitude = latitude,
                    Longitude = longitude,
                    RadiusMeters = radiusMeters,
                    NearbyStopIds = nearbyStops.Select(s => s.Id).ToList(),
                    CreatedAt = DateTime.UtcNow,
                    LastAccessed = DateTime.UtcNow,
                    AccessCount = 1
                };
                
                // Generate geohash values
                cachedLocation.GenerateGeoHashes();
                
                // Save to database
                _dbService.InsertRecord(CollectionNames.CACHED_LOCATIONS, cachedLocation);
                
                // Ensure we don't exceed the maximum number of cached locations
                await PruneOldCachedLocationsAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error caching location {Lat}, {Lon}", latitude, longitude);
            }
        }
        
        /// <summary>
        /// Update an existing cached location
        /// </summary>
        /// <param name="cachedLocation">The cached location to update</param>
        private async Task UpdateCachedLocationAsync(CachedLocation cachedLocation)
        {
            try
            {
                _dbService.UpdateRecord(CollectionNames.CACHED_LOCATIONS, cachedLocation);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating cached location");
            }
        }
        
        /// <summary>
        /// Remove old cached locations if we exceed the maximum number
        /// </summary>
        private async Task PruneOldCachedLocationsAsync()
        {
            try
            {
                // Get all cached locations
                var cachedLocations = _dbService.GetAllRecords<CachedLocation>(CollectionNames.CACHED_LOCATIONS).ToList();
                
                // If we have more than the maximum, remove the oldest ones
                if (cachedLocations.Count > MaxCachedLocations)
                {
                    // Sort by last accessed (oldest first)
                    var locationsToRemove = cachedLocations
                        .OrderBy(l => l.LastAccessed)
                        .Take(cachedLocations.Count - MaxCachedLocations)
                        .ToList();
                    
                    _logger.LogInformation("Pruning {Count} old cached locations", locationsToRemove.Count);
                    
                    // Remove each location
                    foreach (var location in locationsToRemove)
                    {
                        await _dbService.DeleteRecordAsync<CachedLocation>(CollectionNames.CACHED_LOCATIONS, location.Id);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error pruning old cached locations");
            }
        }
        
        /// <summary>
        /// Calculate distance between two points using the Haversine formula
        /// </summary>
        private double CalculateDistanceInMeters(double lat1, double lon1, double lat2, double lon2)
        {
            // Earth radius in meters
            const double earthRadius = 6371000;
            
            // Convert to radians
            var dLat = (lat2 - lat1) * Math.PI / 180;
            var dLon = (lon2 - lon1) * Math.PI / 180;
            
            var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                    Math.Cos(lat1 * Math.PI / 180) * Math.Cos(lat2 * Math.PI / 180) *
                    Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
                    
            var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            
            return earthRadius * c;
        }
        
        /// <summary>
        /// Clear all cached locations
        /// </summary>
        public async Task ClearCachedLocationsAsync()
        {
            try
            {
                // Get all cached locations
                var cachedLocations = _dbService.GetAllRecords<CachedLocation>(CollectionNames.CACHED_LOCATIONS).ToList();
                
                // Remove each location
                foreach (var location in cachedLocations)
                {
                    await _dbService.DeleteRecordAsync<CachedLocation>(CollectionNames.CACHED_LOCATIONS, location.Id);
                }
                
                _logger.LogInformation("Cleared {Count} cached locations", cachedLocations.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error clearing cached locations");
            }
        }
    }
} 