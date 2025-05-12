using System;
using SQLite;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Estimated Time of Arrival data
    /// </summary>
    public class TransportEta
    {
        [PrimaryKey]
        public string Id { get; set; } = Guid.NewGuid().ToString();
        
        [Indexed]
        public string StopId { get; set; } = string.Empty;
        
        [Indexed]
        public string RouteId { get; set; } = string.Empty;
        
        public string RouteNumber { get; set; } = string.Empty;
        
        public string Direction { get; set; } = string.Empty;
        
        public string ServiceType { get; set; } = string.Empty;
        
        public DateTime EtaTime { get; set; }
        
        public DateTime FetchTime { get; set; } = DateTime.Now;
        
        public string RemainingMinutes { get; set; } = string.Empty;
        
        public string Remarks { get; set; } = string.Empty;
        
        public bool IsCancelled { get; set; } = false;
        
        // Determines if the ETA is still valid (not older than 60 seconds)
        [Ignore]
        public bool IsValid
        {
            get
            {
                // Check if ETA time is in the future or less than 60 seconds old
                return EtaTime > DateTime.Now.AddSeconds(-60);
            }
        }
        
        // Computed property for display
        [Ignore]
        public string DisplayEta 
        {
            get 
            {
                if (IsCancelled)
                    return "Cancelled";
                    
                if (!string.IsNullOrEmpty(RemainingMinutes))
                {
                    if (RemainingMinutes == "0")
                        return "Arriving";
                    if (RemainingMinutes == "1")
                        return $"{RemainingMinutes} min";
                    return $"{RemainingMinutes} mins";
                }
                
                // Calculate time difference
                var diff = EtaTime - DateTime.Now;
                if (diff.TotalMinutes <= 0)
                    return "Arriving";
                if (diff.TotalMinutes < 1)
                    return "<1 min";
                    
                int minutes = (int)Math.Round(diff.TotalMinutes);
                if (minutes == 1)
                    return $"{minutes} min";
                return $"{minutes} mins";
            }
        }
    }
} 