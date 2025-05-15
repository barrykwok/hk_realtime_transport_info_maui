using System;
using System.Collections.Generic;
using LiteDB;
using NGeoHash;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Represents a cached location with nearby stops
    /// </summary>
    public class CachedLocation
    {
        [BsonId]
        public string Id { get; set; } = Guid.NewGuid().ToString();
        
        /// <summary>
        /// Latitude of the cached location
        /// </summary>
        public double Latitude { get; set; }
        
        /// <summary>
        /// Longitude of the cached location
        /// </summary>
        public double Longitude { get; set; }
        
        /// <summary>
        /// Display name for this location (optional)
        /// </summary>
        public string Name { get; set; } = string.Empty;
        
        /// <summary>
        /// GeoHash at precision 6 (neighborhood level, ~1.2km x 0.6km)
        /// </summary>
        public string GeoHash6 { get; set; } = string.Empty;
        
        /// <summary>
        /// GeoHash at precision 7 (street level, ~152m x 152m)
        /// </summary>
        public string GeoHash7 { get; set; } = string.Empty;
        
        /// <summary>
        /// GeoHash at precision 8 (building level, ~38m x 19m)
        /// </summary>
        public string GeoHash8 { get; set; } = string.Empty;
        
        /// <summary>
        /// GeoHash at precision 9 (precise, ~4.8m x 4.8m)
        /// </summary>
        public string GeoHash9 { get; set; } = string.Empty;
        
        /// <summary>
        /// List of Stop IDs near this location
        /// </summary>
        public List<string> NearbyStopIds { get; set; } = new List<string>();
        
        /// <summary>
        /// Timestamp when this location was last accessed
        /// </summary>
        public DateTime LastAccessed { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Timestamp when this location was first created
        /// </summary>
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Number of times this location has been accessed
        /// </summary>
        public int AccessCount { get; set; } = 1;

        /// <summary>
        /// Radius in meters used to find nearby stops
        /// </summary>
        public int RadiusMeters { get; set; } = 1000;
        
        /// <summary>
        /// Generate geohash values based on latitude and longitude
        /// </summary>
        public void GenerateGeoHashes()
        {
            if (Latitude != 0 && Longitude != 0)
            {
                GeoHash6 = GeoHash.Encode(Latitude, Longitude, 6);
                GeoHash7 = GeoHash.Encode(Latitude, Longitude, 7);
                GeoHash8 = GeoHash.Encode(Latitude, Longitude, 8);
                GeoHash9 = GeoHash.Encode(Latitude, Longitude, 9);
            }
        }
        
        /// <summary>
        /// Check if the given location is close to this cached location using geohash comparison
        /// </summary>
        /// <param name="latitude">The latitude to check</param>
        /// <param name="longitude">The longitude to check</param>
        /// <param name="precision">Geohash precision to use (6-9)</param>
        /// <returns>True if locations are close, false otherwise</returns>
        public bool IsCloseToLocation(double latitude, double longitude, int precision = 8)
        {
            if (latitude == 0 && longitude == 0) return false;
            
            // Generate geohash for the given location
            string locationGeoHash = GeoHash.Encode(latitude, longitude, precision);
            
            // Compare with cached location geohash at the specified precision
            switch (precision)
            {
                case 6: return locationGeoHash == GeoHash6;
                case 7: return locationGeoHash == GeoHash7;
                case 8: return locationGeoHash == GeoHash8;
                case 9: return locationGeoHash == GeoHash9;
                default: return false;
            }
        }
    }
} 