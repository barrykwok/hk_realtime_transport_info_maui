using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using SQLite;
using NGeoHash;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Transport stop information
    /// Note: The combination of Operator + StopId forms a unique key.
    /// </summary>
    public class TransportStop : INotifyPropertyChanged
    {
        [PrimaryKey]
        public string Id { get; set; }
        
        [Indexed]
        public string StopId { get; set; } = string.Empty;
        
        // Operator will be assigned by the data service
        [Indexed]
        public TransportOperator Operator { get; set; }
        
        /// <summary>
        /// Unique key composed of Operator + StopId
        /// Used for indexing and faster searching
        /// </summary>
        [Indexed]
        public string Key => $"{Operator}_{StopId}";
        
        public string NameEn { get; set; } = string.Empty;
        
        public string NameZh { get; set; } = string.Empty;
        
        // Traditional Chinese name
        public string NameZhHant { get; set; } = string.Empty;
        
        // Simplified Chinese name
        public string NameZhHans { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets the localized name based on current culture
        /// </summary>
        [Ignore]
        public string LocalizedName
        {
            get
            {
                if (!IsZhCulture()) return NameEn;
                
                if (IsSimplifiedChineseCulture())
                {
                    return !string.IsNullOrEmpty(NameZhHans) ? NameZhHans : NameZhHant;
                }
                
                return !string.IsNullOrEmpty(NameZhHant) ? NameZhHant : NameZh;
            }
        }
        
        [Indexed]
        public double Latitude { get; set; }
        
        [Indexed]
        public double Longitude { get; set; }
        
        [Indexed]
        public string GeoHash6 { get; set; } = string.Empty;
        
        [Indexed]
        public string GeoHash7 { get; set; } = string.Empty;
        
        [Indexed]
        public string GeoHash8 { get; set; } = string.Empty;
        
        public string GeoHash9 { get; set; } = string.Empty;
        
        // Last time this stop was updated
        public DateTime LastUpdated { get; set; }
        
        // Routes that have this stop (for UI purposes only)
        [Ignore]
        public List<string> Routes { get; set; } = new List<string>();
        
        // Sequence number for display purposes (populated from RouteStopRelation)
        [Ignore]
        public int Sequence { get; set; }
        
        // First estimated arrival time for display in UI
        private string _firstEta = string.Empty;
        
        [Ignore]
        public string FirstEta
        {
            get => _firstEta;
            set
            {
                if (_firstEta != value)
                {
                    _firstEta = value;
                    OnPropertyChanged();
                }
            }
        }
        
        public TransportStop()
        {
            Id = Guid.NewGuid().ToString();
            LastUpdated = DateTime.UtcNow;
        }
        
        /// <summary>
        /// Check if current culture is Chinese
        /// </summary>
        private bool IsZhCulture()
        {
            string currentCulture = CultureInfo.CurrentUICulture.Name.ToLowerInvariant();
            return currentCulture.StartsWith("zh");
        }
        
        /// <summary>
        /// Check if current Chinese culture is simplified (zh-CN, zh-SG, etc.)
        /// </summary>
        private bool IsSimplifiedChineseCulture()
        {
            string currentCulture = CultureInfo.CurrentUICulture.Name.ToLowerInvariant();
            return currentCulture == "zh-cn" || currentCulture == "zh-sg" || 
                   currentCulture == "zh-hans" || currentCulture.StartsWith("zh-hans-");
        }
        
        /// <summary>
        /// Ensures all geohash fields are correctly populated
        /// </summary>
        public void EnsureGeoHashValues()
        {
            // Skip if coordinates are invalid or empty
            if (Latitude == 0 && Longitude == 0)
                return;
                
            // Calculate and update geohash fields
            GeoHash6 = GeoHash.Encode(Latitude, Longitude, 6);
            GeoHash7 = GeoHash.Encode(Latitude, Longitude, 7);
            GeoHash8 = GeoHash.Encode(Latitude, Longitude, 8);
            GeoHash9 = GeoHash.Encode(Latitude, Longitude, 9);
        }
        
        /// <summary>
        /// Checks if this stop is near the specified coordinates based on geohash proximity
        /// This is a fast first-pass filter to determine if a more precise distance check is needed
        /// </summary>
        /// <param name="lat">Latitude to check against</param>
        /// <param name="lng">Longitude to check against</param>
        /// <param name="precision">GeoHash precision to check (6-9)</param>
        /// <param name="prefixLength">Length of prefix to match</param>
        /// <returns>True if potentially near, false otherwise</returns>
        public bool IsNearByGeoHash(double lat, double lng, int precision = 7, int prefixLength = 3)
        {
            // Validate parameters
            if (precision < 6 || precision > 9 || prefixLength < 1 || prefixLength > 5)
                return false;
                
            // Skip if no coordinates
            if (Latitude == 0 && Longitude == 0)
                return false;
                
            string locationGeoHash = GeoHash.Encode(lat, lng, precision);
            string stopGeoHash = "";
                
            // Use the matching precision geohash
            switch (precision)
            {
                case 6: stopGeoHash = GeoHash6; break;
                case 7: stopGeoHash = GeoHash7; break;
                case 8: stopGeoHash = GeoHash8; break;
                case 9: stopGeoHash = GeoHash9; break;
            }
                
            // If geohash isn't available, compute it on the fly
            if (string.IsNullOrEmpty(stopGeoHash))
            {
                stopGeoHash = GeoHash.Encode(Latitude, Longitude, precision);
            }
                
            // Make sure we have enough characters to compare
            if (locationGeoHash.Length < prefixLength || stopGeoHash.Length < prefixLength)
                return false;
                
            // Compare geohash prefixes
            return locationGeoHash.Substring(0, prefixLength) == stopGeoHash.Substring(0, prefixLength);
        }
        
        // INotifyPropertyChanged implementation
        public event PropertyChangedEventHandler? PropertyChanged;
        
        protected virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName ?? string.Empty));
        }
    }
} 