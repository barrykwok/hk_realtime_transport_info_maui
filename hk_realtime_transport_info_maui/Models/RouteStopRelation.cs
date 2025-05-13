using System;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Represents the many-to-many relationship between TransportRoute and TransportStop.
    /// This model stores the sequence of stops within a specific route, allowing the same
    /// stop to be used in multiple routes with different sequence positions.
    /// </summary>
    public class RouteStopRelation
    {
        public int Id { get; set; }
        
        public string RouteId { get; set; }
        
        public string StopId { get; set; }
        
        // The sequence number of this stop in this specific route
        public int Sequence { get; set; }
        
        // Optional direction information if the route has multiple directions
        public string Direction { get; set; }
        
        // Last time this relationship was updated
        public DateTime LastUpdated { get; set; }
        
        // Navigation properties - we'll manually load these as needed
        public TransportRoute? Route { get; set; }
        
        public TransportStop? Stop { get; set; }
        
        public RouteStopRelation()
        {
            RouteId = string.Empty;
            StopId = string.Empty;
            Direction = string.Empty;
            LastUpdated = DateTime.UtcNow;
        }
    }
} 