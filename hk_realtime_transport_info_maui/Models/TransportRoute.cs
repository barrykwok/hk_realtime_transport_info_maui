using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using SQLite;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Main transport route class. 
    /// Note: The combination of Operator + RouteNumber + ServiceType + Bound forms a unique key.
    /// </summary>
    public class TransportRoute : INotifyPropertyChanged
    {
        [PrimaryKey]
        public string Id { get; set; }
        
        [Indexed]
        public string RouteNumber { get; set; } = string.Empty;
        
        public string RouteName { get; set; } = string.Empty;
        
        [Indexed]
        public TransportType Type { get; set; }
        
        // Primary operator of this route - assigned by data service
        [Indexed]
        public TransportOperator Operator { get; set; }
        
        /// <summary>
        /// Unique key composed of Operator + RouteNumber + ServiceType + Bound
        /// Used for indexing and faster searching
        /// </summary>
        [Indexed]
        public string Key => $"{Operator}_{RouteNumber}_{ServiceType}_{Bound}";
        
        public string OriginEn { get; set; } = string.Empty;
        
        public string OriginZh { get; set; } = string.Empty;
        
        // Traditional Chinese origin
        public string OriginZhHant { get; set; } = string.Empty;
        
        // Simplified Chinese origin
        public string OriginZhHans { get; set; } = string.Empty;
        
        public string DestinationEn { get; set; } = string.Empty;
        
        public string DestinationZh { get; set; } = string.Empty;
        
        // Traditional Chinese destination
        public string DestinationZhHant { get; set; } = string.Empty;
        
        // Simplified Chinese destination
        public string DestinationZhHans { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets the localized origin based on current culture
        /// </summary>
        [Ignore]
        public string LocalizedOrigin 
        { 
            get 
            {
                string result;
                
                if (!IsZhCulture()) 
                    result = OriginEn;
                else if (IsSimplifiedChineseCulture())
                    result = !string.IsNullOrEmpty(OriginZhHans) ? OriginZhHans : OriginZhHant;
                else
                    result = !string.IsNullOrEmpty(OriginZhHant) ? OriginZhHant : OriginZh;
                
                // Remove any {0} placeholders
                return result?.Replace("{0}", "").Trim() ?? string.Empty;
            }
        }
        
        /// <summary>
        /// Gets the localized destination based on current culture
        /// </summary>
        [Ignore]
        public string LocalizedDestination 
        { 
            get 
            {
                string result;
                
                if (!IsZhCulture()) 
                    result = DestinationEn;
                else if (IsSimplifiedChineseCulture())
                    result = !string.IsNullOrEmpty(DestinationZhHans) ? DestinationZhHans : DestinationZhHant;
                else
                    result = !string.IsNullOrEmpty(DestinationZhHant) ? DestinationZhHant : DestinationZh;
                
                // Remove any {0} placeholders
                return result?.Replace("{0}", "").Trim() ?? string.Empty;
            }
        }
        
        [Indexed]
        public string ServiceType { get; set; } = string.Empty;
        
        [Indexed]
        public string Bound { get; set; } = string.Empty;

        public string NlbId { get; set; } = string.Empty;
        
        public string GtfsId { get; set; } = string.Empty;
        
        public bool IsCircular { get; set; }
        
        // Route-stop relationships
        [Ignore]
        public List<RouteStopRelation> StopRelations { get; set; } = new List<RouteStopRelation>();
        
        private List<TransportStop> _stops = new List<TransportStop>();
        
        /// <summary>
        /// Collection of stops for this route.
        /// Uses lazy loading - the stops will only be loaded from the database when needed.
        /// </summary>
        [Ignore]
        public List<TransportStop> Stops 
        { 
            get => _stops; 
            set => _stops = value; 
        }
        
        /// <summary>
        /// Flag to indicate if stops have been loaded
        /// </summary>
        [Ignore]
        public bool StopsLoaded => _stops.Count > 0;
        
        public DateTime LastUpdated { get; set; }
        
        public string Fare { get; set; } = string.Empty;
        
        public string JourneyTime { get; set; } = string.Empty;
        
        // ETA related properties
        private TransportEta? _firstEta;
        
        [Ignore]
        public TransportEta? FirstEta 
        { 
            get => _firstEta; 
            set 
            {
                if (_firstEta == value) return;
                
                _firstEta = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(HasEta));
                OnPropertyChanged(nameof(FirstEtaDisplay));
            }
        }
        
        [Ignore]
        public bool HasEta => FirstEta != null;
        
        [Ignore]
        public string FirstEtaDisplay => FirstEta?.DisplayEta ?? string.Empty;
        
        // Constructor with required fields
        public TransportRoute()
        {
            Id = Guid.NewGuid().ToString();
            LastUpdated = DateTime.UtcNow;
        }
        
        /// <summary>
        /// Check if current culture is Chinese
        /// </summary>
        private bool IsZhCulture()
        {
            var culture = CultureInfo.CurrentUICulture.Name;
            return culture.StartsWith("zh", StringComparison.OrdinalIgnoreCase);
        }
        
        /// <summary>
        /// Check if current culture is simplified Chinese
        /// </summary>
        private bool IsSimplifiedChineseCulture()
        {
            var culture = CultureInfo.CurrentUICulture.Name;
            return culture.StartsWith("zh-Hans", StringComparison.OrdinalIgnoreCase) || 
                   culture.StartsWith("zh-CN", StringComparison.OrdinalIgnoreCase) ||
                   culture.StartsWith("zh-SG", StringComparison.OrdinalIgnoreCase);
        }

        // INotifyPropertyChanged implementation
        public event PropertyChangedEventHandler? PropertyChanged;
        
        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = "")
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
} 