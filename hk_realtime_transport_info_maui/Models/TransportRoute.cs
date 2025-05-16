using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Main transport route class. 
    /// Note: The combination of Operator + RouteNumber + ServiceType + Bound forms a unique key.
    /// </summary>
    public class TransportRoute : INotifyPropertyChanged
    {
        public string Id { get; set; }
        
        public string RouteNumber { get; set; } = string.Empty;
        
        public string RouteName { get; set; } = string.Empty;
        
        public TransportType Type { get; set; }
        
        // Primary operator of this route - assigned by data service
        public TransportOperator Operator { get; set; }
        
        /// <summary>
        /// Unique key composed of Operator + RouteNumber + ServiceType + Bound
        /// Used for indexing and faster searching
        /// </summary>
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
        
        public string ServiceType { get; set; } = string.Empty;
        
        public string Bound { get; set; } = string.Empty;

        public string NlbId { get; set; } = string.Empty;
        
        public string GtfsId { get; set; } = string.Empty;
        
        public bool IsCircular { get; set; }
        
        // Route-stop relationships
        public List<RouteStopRelation> StopRelations { get; set; } = new List<RouteStopRelation>();
        
        // All stops for this route (for UI purposes only)
        public List<TransportStop> Stops { get; set; } = new List<TransportStop>();
        
        // Frequency info
        public string FrequencyWeekday { get; set; } = string.Empty;
        
        public string FrequencySaturday { get; set; } = string.Empty;
        
        public string FrequencySunday { get; set; } = string.Empty;
        
        public string FrequencyHoliday { get; set; } = string.Empty;
        
        // Fares
        public string Fares { get; set; } = string.Empty;
        
        // Special route info
        public string SpecialRouteType { get; set; } = string.Empty;
        
        public string SpecialServiceDays { get; set; } = string.Empty;
        
        public DateTime StartDate { get; set; } = DateTime.MinValue;
        
        public DateTime EndDate { get; set; } = DateTime.MaxValue;
        
        public string TerminalStation { get; set; } = string.Empty;
        
        // Color for UI display
        private string _displayColor = string.Empty;
        public string DisplayColor 
        { 
            get
            {
                if (string.IsNullOrEmpty(_displayColor))
                {
                    // Use a deterministic hash algorithm so the same route always gets the same color
                    return GenerateRouteColor();
                }
                return _displayColor;
            }
            set
            {
                if (_displayColor != value)
                {
                    _displayColor = value;
                    OnPropertyChanged();
                }
            }
        }
        
        // First ETA for quick display in UI
        private string _firstEta = string.Empty;
        
        public string FirstEta
        {
            get => _firstEta;
            set
            {
                if (_firstEta != value)
                {
                    _firstEta = value;
                    OnPropertyChanged();
                    OnPropertyChanged(nameof(HasEta));
                    OnPropertyChanged(nameof(FirstEtaDisplay));
                }
            }
        }
        
        /// <summary>
        /// Formatted ETA display for the UI
        /// </summary>
        public string FirstEtaDisplay 
        { 
            get 
            {
                // For debugging - REMOVED: System.Diagnostics.Debug.WriteLine($"FirstEta for {RouteNumber}: '{FirstEta}'");
                return !string.IsNullOrEmpty(FirstEta) ? FirstEta : string.Empty;
            }
        }
        
        /// <summary>
        /// Indicates whether this route has any ETAs
        /// </summary>
        public bool HasEta 
        {
            get 
            {
                // Only return true if FirstEta exists AND the ETA is for a future time
                // (NextEta contains the actual ETA object with timestamp)
                var hasEta = !string.IsNullOrEmpty(FirstEta) && (NextEta?.IsActive ?? false);
                // For debugging - REMOVED: System.Diagnostics.Debug.WriteLine($"HasEta for {RouteNumber}: {hasEta}");
                return hasEta;
            }
        }
        
        // Last time this route was updated
        public DateTime LastUpdated { get; set; }
        
        // Add ETA properties needed by UpdateRouteEtas
        public TransportEta NextEta { get; set; }
        public List<TransportEta> Etas { get; set; } = new List<TransportEta>();
        public string FirstEtaText => NextEta != null ? NextEta.RemainingMinutes : "-";
        public string FirstEtaMinutes => NextEta != null ? $"{NextEta.RemainingMinutes}m" : "-";
        public bool HasEtas => Etas.Count > 0;
        
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
        /// Generates a color for the route based on its route number and operator for consistency
        /// </summary>
        private string GenerateRouteColor()
        {
            // Generate a deterministic color based on the route number and operator
            int hashCode = (RouteNumber + Operator.ToString()).GetHashCode();
            
            // Use the hash to generate RGB values, ensuring they're visible (not too dark/light)
            var r = ((hashCode & 0xFF0000) >> 16) % 200 + 50;
            var g = ((hashCode & 0x00FF00) >> 8) % 200 + 50;
            var b = (hashCode & 0x0000FF) % 200 + 50;
            
            // Ensure subway/metro lines get their correct colors
            if (Type == TransportType.MTR && Operator == TransportOperator.MTR)
            {
                // Handle MTR line colors
                if (RouteNumber == "TML")
                    return "#9C2E00"; // Tuen Ma Line - Brown
                else if (RouteNumber == "TKL")
                    return "#7E3C99"; // Tseung Kwan O Line - Purple
                else if (RouteNumber == "TCL")
                    return "#F7943E"; // Tung Chung Line - Orange
                else if (RouteNumber == "AEL")
                    return "#00888E"; // Airport Express - Teal
                else if (RouteNumber == "EAL")
                    return "#5EB9E5"; // East Rail Line - Light Blue
                else if (RouteNumber == "TWL")
                    return "#C41E3A"; // Tsuen Wan Line - Red
                else if (RouteNumber == "ISL")
                    return "#0075C1"; // Island Line - Blue
                else if (RouteNumber == "KTL")
                    return "#00A040"; // Kwun Tong Line - Green
                else if (RouteNumber == "SIL")
                    return "#CBD300"; // South Island Line - Lime Green
                else if (RouteNumber == "DRL")
                    return "#E60012"; // Disney Resort Line - Red
            }
            
            // Return as hex color
            return $"#{r:X2}{g:X2}{b:X2}";
        }
        
        /// <summary>
        /// Get a simple display name for the route that includes the route number and destinations
        /// </summary>
        /// <returns>A formatted string with route details</returns>
        public string GetDisplayName()
        {
            return $"{RouteNumber}: {LocalizedOrigin} → {LocalizedDestination}";
        }
        
        /// <summary>
        /// Get a simple display name for the route that just shows the route number
        /// </summary>
        /// <returns>The route number</returns>
        public string GetShortDisplayName()
        {
            return RouteNumber;
        }
        
        /// <summary>
        /// Get the most appropriate frequency string based on the current day of week and holiday status
        /// </summary>
        /// <param name="isHoliday">Whether today is a holiday</param>
        /// <returns>The appropriate frequency string</returns>
        public string GetCurrentFrequency(bool isHoliday = false)
        {
            if (isHoliday && !string.IsNullOrEmpty(FrequencyHoliday))
                return FrequencyHoliday;
                
            DayOfWeek dayOfWeek = DateTime.Now.DayOfWeek;
            
            if (dayOfWeek == DayOfWeek.Sunday)
                return !string.IsNullOrEmpty(FrequencySunday) ? FrequencySunday : FrequencyWeekday;
            else if (dayOfWeek == DayOfWeek.Saturday)
                return !string.IsNullOrEmpty(FrequencySaturday) ? FrequencySaturday : FrequencyWeekday;
            else
                return FrequencyWeekday;
        }
        
        // INotifyPropertyChanged implementation
        public event PropertyChangedEventHandler? PropertyChanged;
        
        public virtual void OnPropertyChanged([CallerMemberName] string? propertyName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName ?? string.Empty));
        }
    }
} 