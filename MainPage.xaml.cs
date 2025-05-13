using System;
using System.Threading.Tasks;
using Xamarin.Essentials;
using Microsoft.Extensions.Logging;
using Xamarin.Forms;

namespace YourNamespace
{
	public partial class MainPage : ContentPage
	{
		private readonly ILogger<MainPage> _logger;
		private readonly double LocationRefreshIntervalSeconds = 60;
		private DistanceFilter _currentDistanceFilter = DistanceFilter.All;
		private DateTime _lastLocationUpdate = DateTime.MinValue;
		private Location _userLocation;
		private readonly StopGroupsCache _stopGroupsCache = new StopGroupsCache();

		public MainPage(ILogger<MainPage> logger)
		{
			_logger = logger;
			InitializeComponent();
		}

		private async Task UpdateUserLocationIfNeeded()
		{
			try
			{
				// Check location permissions first
				var status = await Permissions.CheckStatusAsync<Permissions.LocationWhenInUse>();
				if (status != PermissionStatus.Granted)
				{
					status = await Permissions.RequestAsync<Permissions.LocationWhenInUse>();
					if (status != PermissionStatus.Granted)
					{
						// If user denied permission, default to ALL filter
						_currentDistanceFilter = DistanceFilter.All;
						
						// Update button colors
						UpdateDistanceFilterButtonColors();
						
						_logger?.LogInformation("Location permission denied, defaulting to ALL filter");
						return;
					}
				}
				
				// Only update location if it's been more than 60 seconds since last update
				if (_userLocation == null || DateTime.Now - _lastLocationUpdate > TimeSpan.FromSeconds(LocationRefreshIntervalSeconds))
				{
					// Always run geolocation operations on a background thread
					await Task.Run(async () => {
						try 
						{
							var request = new GeolocationRequest(GeolocationAccuracy.Medium, TimeSpan.FromSeconds(10));
							var location = await Geolocation.GetLocationAsync(request);
							
							if (location != null)
							{
								// Process location update on main thread
								await MainThread.InvokeOnMainThreadAsync(async () => {
									_userLocation = location;
									_lastLocationUpdate = DateTime.Now;
									
									// Clear cache when location changes
									_stopGroupsCache.Clear();
									
									// After getting new location, update nearby stops
									await UpdateNearbyStops();
									
									// Apply the current distance filter with loading indicator
									IsRefreshing = true;
									
									try
									{
										await ApplyDistanceFilterAsync(_currentDistanceFilter, _currentDistanceFilter);
									}
									finally
									{
										IsRefreshing = false;
									}
								});
							}
							else
							{
								await MainThread.InvokeOnMainThreadAsync(() => {
									_logger?.LogWarning("Could not get location, defaulting to ALL filter");
									_currentDistanceFilter = DistanceFilter.All;
									UpdateDistanceFilterButtonColors();
									FilterRoutes();
								});
							}
						}
						catch (Exception ex)
						{
							_logger?.LogError(ex, "Error getting user location in background task");
							
							// On error, default to ALL filter
							await MainThread.InvokeOnMainThreadAsync(() => {
								_currentDistanceFilter = DistanceFilter.All;
								UpdateDistanceFilterButtonColors();
								FilterRoutes();
							});
						}
					});
				}
			}
			catch (Exception ex)
			{
				_logger?.LogError(ex, "Error getting user location");
				
				// On error, default to ALL filter
				_currentDistanceFilter = DistanceFilter.All;
				UpdateDistanceFilterButtonColors();
				FilterRoutes();
			}
		}
	}
} 