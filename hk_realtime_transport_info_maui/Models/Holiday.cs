using System;

namespace hk_realtime_transport_info_maui.Models
{
    /// <summary>
    /// Holiday information for schedule determination
    /// </summary>
    public class Holiday
    {
        public string Id { get; set; } = string.Empty;
        
        public DateTime Date { get; set; }
        
        public string Name { get; set; } = string.Empty;
        
        public bool IsHoliday { get; set; }
        
        public Holiday()
        {
            Id = Guid.NewGuid().ToString();
        }
    }
} 