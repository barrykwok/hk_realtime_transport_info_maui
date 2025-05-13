using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using hk_realtime_transport_info_maui.Models;
using System.Linq;

namespace hk_realtime_transport_info_maui
{
    // StopGroup class to act as an expander model with ETA support
    public class StopGroup : INotifyPropertyChanged
    {
        public TransportStop Stop { get; private set; }
        
        // Change to lazy loading for routes collection
        private ObservableCollection<TransportRoute>? _routes;
        public ObservableCollection<TransportRoute> Routes 
        { 
            get 
            { 
                // Lazy initialization of routes
                if (_routes == null && _routesList != null)
                {
                    _routes = new ObservableCollection<TransportRoute>(_routesList);
                    // Clear the backing list to free memory after initialization
                    _routesList = null;
                }
                return _routes ?? new ObservableCollection<TransportRoute>();
            }
        }
        
        // Backing field for routes - more memory efficient than ObservableCollection when not displayed
        private List<TransportRoute>? _routesList;
        
        public double DistanceInMeters { get; private set; }
        
        // Dictionary to store ETAs for each route by route number and service type
        private Dictionary<string, List<TransportEta>> _routeEtas = new Dictionary<string, List<TransportEta>>(StringComparer.OrdinalIgnoreCase);
        
        private bool _isExpanded;
        public bool IsExpanded
        {
            get => _isExpanded;
            set 
            {
                if (_isExpanded == value) return;
                
                _isExpanded = value;
                OnPropertyChanged();
                
                // If we're expanding and routes haven't been loaded yet, initialize them
                if (_isExpanded && _routes == null && _routesList != null)
                {
                    _routes = new ObservableCollection<TransportRoute>(_routesList);
                    // Clear the backing list to free memory
                    _routesList = null;
                    OnPropertyChanged(nameof(Routes));
                }
                
                // Also notify dependent properties
                OnPropertyChanged(nameof(HeaderText));
                OnPropertyChanged(nameof(HasNoVisibleRoutes));
            }
        }

        public string HeaderText => $"{Stop.LocalizedName} {(IsExpanded ? "▼" : "►")}";
        public string DistanceText => $"{Math.Round(DistanceInMeters)}m";
        
        // Use backing fields for expensive calculations
        private int _routesWithEtaCount = 0;
        public string RouteCountText => $"{_routesWithEtaCount} routes";

        public StopGroup(TransportStop stop, IEnumerable<TransportRoute> routes, double distanceInMeters)
        {
            Stop = stop;
            // Store as list initially for memory efficiency - will convert to ObservableCollection when expanded
            _routesList = routes?.ToList() ?? new List<TransportRoute>();
            DistanceInMeters = distanceInMeters;
            IsExpanded = true; // Start expanded by default
            
            // Pre-calculate routes with ETAs - protect against null
            _routesWithEtaCount = _routesList?.Count(r => r?.HasEta == true) ?? 0;
        }

        /// <summary>
        /// Updates the ETAs for a specific route in this stop group
        /// </summary>
        public void UpdateEtas(string routeNumber, string serviceType, List<TransportEta> etas)
        {
            if (string.IsNullOrEmpty(routeNumber) || etas == null)
            {
                return;
            }
            
            // Create a key from route number and service type
            string key = $"{routeNumber}_{serviceType}";
            
            bool hasEtasBefore = HasEtas;
            
            // Update the ETAs for this route
            _routeEtas[key] = etas;
            
            // Update the count of routes with ETAs
            if (_routes != null)
            {
                _routesWithEtaCount = _routes.Count(r => r?.HasEta == true);
            }
            else if (_routesList != null)
            {
                _routesWithEtaCount = _routesList.Count(r => r?.HasEta == true);
            }
            
            // Notify UI only if the HasEtas property actually changed
            if (hasEtasBefore != HasEtas)
            {
                OnPropertyChanged(nameof(HasEtas));
            }
            
            OnPropertyChanged(nameof(HasNoVisibleRoutes));
            OnPropertyChanged(nameof(RouteCountText));
        }
        
        /// <summary>
        /// Gets whether any routes in this stop group have ETAs
        /// </summary>
        public bool HasEtas => _routeEtas != null && _routeEtas.Count > 0 && _routeEtas.Any(r => r.Value != null && r.Value.Count > 0);

        /// <summary>
        /// Returns true when the group is expanded but none of the routes have ETAs
        /// </summary>
        public bool HasNoVisibleRoutes
        {
            get
            {
                if (!IsExpanded) return false;
                
                // Check if any routes explicitly have ETAs, allowing routes without ETAs to be visible
                if (_routes != null && _routes.Any())
                {
                    var visibleRoutesCount = _routes.Count(r => r != null);
                    return visibleRoutesCount == 0;
                }
                else if (_routesList != null && _routesList.Any())
                {
                    var visibleRoutesCount = _routesList.Count(r => r != null);
                    return visibleRoutesCount == 0;
                }
                
                return true;
            }
        }

        /// <summary>
        /// Returns true when at least one route has a valid ETA
        /// </summary>
        public bool HasAnyVisibleRoutesWithEta => _routesWithEtaCount > 0;

        // Get ETA for a specific route
        public TransportEta? GetEtaForRoute(string routeNumber, string serviceType)
        {
            string key = $"{routeNumber}_{serviceType}";
            if (_routeEtas.TryGetValue(key, out var etas) && etas.Count > 0)
            {
                // Return the first (earliest) ETA
                return etas[0];
            }
            
            return null;
        }

        // Get all ETAs for a route (for cases where we want to show multiple ETAs)
        public List<TransportEta> GetAllEtasForRoute(string routeNumber, string serviceType)
        {
            string key = $"{routeNumber}_{serviceType}";
            if (_routeEtas.TryGetValue(key, out var etas))
            {
                return etas;
            }
            
            return new List<TransportEta>();
        }
        
        /// <summary>
        /// Updates the distance to this stop from the current location
        /// </summary>
        public void UpdateDistance(double newDistanceInMeters)
        {
            if (Math.Abs(DistanceInMeters - newDistanceInMeters) > 1)  // Only update if changed by more than 1 meter
            {
                DistanceInMeters = newDistanceInMeters;
                OnPropertyChanged(nameof(DistanceInMeters));
                OnPropertyChanged(nameof(DistanceText));
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        protected virtual void OnPropertyChanged([CallerMemberName] string propertyName = "") =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
            
        /// <summary>
        /// Public method to notify property changed for external callers
        /// </summary>
        public void NotifyPropertyChanged(string propertyName)
        {
            OnPropertyChanged(propertyName);
        }
    }
} 