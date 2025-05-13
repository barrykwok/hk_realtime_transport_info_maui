using System;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// User favorite routes
    /// </summary>
    public class FavoriteRoute
    {
        public string Id { get; set; }
        
        public string RouteId { get; set; } = string.Empty;
        
        public string StopId { get; set; } = string.Empty;
        
        public int Order { get; set; }
        
        public DateTime Created { get; set; }
        
        public FavoriteRoute()
        {
            Id = Guid.NewGuid().ToString();
            Created = DateTime.UtcNow;
        }
    }
} 