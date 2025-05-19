using System;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Estimated Time of Arrival data
    /// </summary>
    public class TransportEta
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        
        public string StopId { get; set; } = string.Empty;
        
        public string RouteId { get; set; } = string.Empty;
        
        public string RouteNumber { get; set; } = string.Empty;
        
        public string Direction { get; set; } = string.Empty;
        
        public string ServiceType { get; set; } = string.Empty;
        
        public DateTime EtaTime { get; set; }
        
        public DateTime FetchTime { get; set; } = DateTime.Now;
        
        public string RemainingMinutes { get; set; } = string.Empty;
        
        public string Remarks { get; set; } = string.Empty;
        
        public bool IsCancelled { get; set; } = false;
        
        // Sequence number of the stop in the route (for KMB route ETAs that only provide sequence)
        public int Sequence { get; set; }
        
        // Determines if the ETA is still valid (not older than 60 seconds)
        public bool IsValid
        {
            get
            {
                // Check if ETA time is in the future or less than 60 seconds old
                return EtaTime > DateTime.Now.AddSeconds(-60);
            }
        }
        
        // Computed property for display
        public string DisplayEta 
        {
            get 
            {
                if (IsCancelled)
                    return "Cancelled";
                
                var diff = EtaTime - DateTime.Now;
                
                if (diff.TotalMinutes <= 1)
                    return "Arriving";
                else if (diff.TotalHours >= 1)
                    return $"{Math.Floor(diff.TotalHours)}h {diff.Minutes}m";
                else
                    return $"{diff.Minutes} min";
            }
        }
    }
} 