using System;
using SQLite;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// User favorite routes
    /// </summary>
    public class FavoriteRoute
    {
        [PrimaryKey]
        public string Id { get; set; }
        
        [Indexed]
        public string RouteId { get; set; } = string.Empty;
        
        [Indexed]
        public string StopId { get; set; } = string.Empty;
        
        [Indexed]
        public int Order { get; set; }
        
        public DateTime Created { get; set; }
        
        public FavoriteRoute()
        {
            Id = Guid.NewGuid().ToString();
            Created = DateTime.UtcNow;
        }
    }
} 