using System;

namespace hk_realtime_transport_info_maui.Models
{
    public enum TransportType
    {
        Bus,
        MiniBus,
        MTR,
        LightRail,
        Ferry,
        LRTFeeder
    }

    public enum TransportOperator
    {
        KMB,    // Kowloon Motor Bus
        CTB,    // Citybus
        NLB,    // New Lantao Bus
        GMB,    // Green Minibus
        MTR,    // Mass Transit Railway
        LRT,    // Light Rail Transit
        SF,     // Sun Ferry
        FF,     // Fortune Ferry
        HKKF    // Hong Kong & Kowloon Ferry
    }
} 