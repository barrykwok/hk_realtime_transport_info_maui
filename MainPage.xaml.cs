/// <summary>
/// Handles clicks on the distance filter buttons
/// </summary>
private void OnDistanceFilterClicked(object sender, EventArgs e)
{
	if (sender is Button button)
	{
		DistanceFilter newFilter = DistanceFilter.All;
		
		switch (button.Text)
		{
			case "100m":
				newFilter = DistanceFilter.Meters100;
				break;
			case "200m":
				newFilter = DistanceFilter.Meters200;
				break;
			case "400m":
				newFilter = DistanceFilter.Meters400;
				break;
			case "600m":
				newFilter = DistanceFilter.Meters600;
				break;
			case "ALL":
				newFilter = DistanceFilter.All;
				break;
		}
		
		// Only proceed if the filter changed
		if (_currentDistanceFilter != newFilter)
		{
			_logger?.LogInformation("Changing distance filter from {oldFilter} to {newFilter}", _currentDistanceFilter, newFilter);
			
			// Store previous filter before updating
			var previousFilter = _currentDistanceFilter;
			_currentDistanceFilter = newFilter;
			
			// Immediately update button colors to provide visual feedback
			UpdateDistanceFilterButtonColors();
			
			// Toggle visibility of views based on the selected filter
			bool isNearbyFilter = newFilter != DistanceFilter.All;
			IsNearbyViewVisible = isNearbyFilter;
			IsAllViewVisible = !isNearbyFilter;
			
			// Get the appropriate collection view based on the selected filter
			CollectionView? collectionView = isNearbyFilter 
				? this.FindByName<CollectionView>("NearbyRoutesCollection")
				: this.FindByName<CollectionView>("AllRoutesCollection");
			
			// Suspend UI updates by starting a batch operation
			if (collectionView != null)
			{
				collectionView.BatchBegin();
			}
			
			// Add a loading indicator to show progress
			ActivityIndicator loadingIndicator = new ActivityIndicator
			{ 
				IsRunning = true,
				HorizontalOptions = LayoutOptions.Center,
				VerticalOptions = LayoutOptions.Center,
				Color = Color.FromArgb("#4B0082") // Same as selected button color
			};
					
			if (collectionView != null && collectionView.Parent is Grid grid)
			{
				// Add the indicator at the center of the collection view
				grid.Add(loadingIndicator);
				Grid.SetRow(loadingIndicator, Grid.GetRow(collectionView));
				Grid.SetColumn(loadingIndicator, Grid.GetColumn(collectionView));
				
				// Show progress indicator but don't block UI with refresh indicator
				loadingIndicator.IsVisible = true;
			}
			
			// Apply the filter in the background to keep UI responsive
			_ = Task.Run(async () => 
			{
				try 
				{
					if (isNearbyFilter)
					{
						// Handle nearby filter
						var nearbyResult = await ApplyDistanceFilterAsync(previousFilter, _currentDistanceFilter);
						await MainThread.InvokeOnMainThreadAsync(() => 
						{
							// Update Routes property inside BatchBegin/BatchCommit
							Routes = nearbyResult;
							
							// Resume UI updates after data is ready
							if (collectionView != null)
							{
								collectionView.BatchCommit();
							}
						});
					}
					else
					{
						// For ALL filter - use highly optimized approach to prevent UI freezing
						await LoadAllRoutesWithVirtualization();
						
						// Resume UI updates after ALL routes are loaded
						await MainThread.InvokeOnMainThreadAsync(() => 
						{
							if (collectionView != null)
							{
								collectionView.BatchCommit();
							}
						});
					}
				}
				finally 
				{
					// Remove loading indicator when done
					await MainThread.InvokeOnMainThreadAsync(() => 
					{
						if (collectionView != null && collectionView.Parent is Grid parentGrid && loadingIndicator.Parent == parentGrid)
						{
							parentGrid.Remove(loadingIndicator);
						}
					});
				}
			});
		}
	}
}

/// <summary>
/// Applies the selected distance filter asynchronously
/// </summary>
private async Task<ObservableRangeCollection<object>> ApplyDistanceFilterAsync(DistanceFilter previousFilter, DistanceFilter filter)
{
	try
	{
		// If user location is null or we're showing all routes, just use standard filtering
		if (_userLocation == null || filter == DistanceFilter.All)
		{
			var resultsList = FilterRoutesInternal(); // Returns List<object>
			
			// If this is the All filter and we need all routes, make sure we're getting everything
			if (filter == DistanceFilter.All && string.IsNullOrEmpty(_searchQuery))
			{
				// Load all routes directly instead of just the initial batch
				resultsList = _allRoutes.Cast<object>().ToList();
			}
			
			await MainThread.InvokeOnMainThreadAsync(() => {
				KeyboardManager.UpdateAvailableRouteChars(resultsList.OfType<TransportRoute>().ToList(), _searchQuery);
			});
			
			// Use ObservableRangeCollection instead of ObservableCollection
			var result = new ObservableRangeCollection<object>();
			// Use batch operation to suppress notifications until all items are added
			result.BeginBatch();
			result.AddRange(resultsList);
			result.EndBatch();
			return result;
		}
		
		// If nearby stops haven't been loaded yet, update them
		if (_stopsNearby.Count == 0)
		{
			await UpdateNearbyStops();
		}
		
		// Check if we have a cached result
		if (_stopGroupsCache.ContainsKey(filter) && string.IsNullOrEmpty(_searchQuery))
		{
			return _stopGroupsCache[filter];
		}
		
		// Check if we can perform an incremental update
		bool canDoIncrementalUpdate = string.IsNullOrEmpty(_searchQuery) && 
			_stopGroupsCache.ContainsKey(previousFilter) &&
			previousFilter != DistanceFilter.All && 
			filter != DistanceFilter.All;

		ObservableRangeCollection<object> filteredItems;
		
		if (canDoIncrementalUpdate)
		{
			// Get previous filter's groups
			filteredItems = await IncrementallyUpdateStopGroups(previousFilter, filter);
		}
		else
		{
			// Regular full processing without triggering data downloads
			// Get the list of nearby stops for the selected distance
			List<TransportStop> nearbyStops = new List<TransportStop>();
			
			string distanceKey = "";
			switch (filter)
			{
				case DistanceFilter.Meters100:
					distanceKey = "100m";
					break;
				case DistanceFilter.Meters200:
					distanceKey = "200m";
					break;
				case DistanceFilter.Meters400:
					distanceKey = "400m";
					break;
				case DistanceFilter.Meters600:
					distanceKey = "600m";
					break;
			}
			
			// Safely get stops for the selected distance
			if (!string.IsNullOrEmpty(distanceKey) && _stopsNearby.ContainsKey(distanceKey))
			{
				nearbyStops = _stopsNearby[distanceKey];
			}
			
			// Prepare UI update in a separate step
			filteredItems = new ObservableRangeCollection<object>();
			// Start batch operation to suppress notifications
			filteredItems.BeginBatch();
			
			int totalRoutes = 0;
			string noRoutesText = string.Empty;
			
			// If no nearby stops found, show empty list
			if (nearbyStops.Count == 0)
			{
				noRoutesText = $"No stops found within {filter.ToString().Replace("Meters", "")} of your location.";
			}
			else
			{
				// Dictionary to group routes by stop
				var routesByStop = new Dictionary<string, Tuple<TransportStop, List<TransportRoute>>>();
				
				// First collect all stop data without UI updating to reduce UI impact
				var stopGroups = new List<StopGroup>();
				
				// Get a list of stop IDs for batch processing
				var stopIds = nearbyStops.Select(s => s.Id).ToList();
				
				// Process each stop
				foreach (var stop in nearbyStops)
				{
					// Get routes for this stop from database
					var routesForStop = await _databaseService.GetRoutesForStopAsync(stop.Id);
					
					// Apply text search filter if needed
					if (!string.IsNullOrEmpty(_searchQuery))
					{
						routesForStop = routesForStop.Where(r => 
							r.RouteNumber.Contains(_searchQuery, StringComparison.OrdinalIgnoreCase)).ToList();
					}
					
					// If this stop has matching routes, process it
					if (routesForStop.Any())
					{
						// Calculate distance from user to this stop
						var stopLocation = new Location(stop.Latitude, stop.Longitude);
						double distanceInMeters = Location.CalculateDistance(_userLocation, stopLocation, DistanceUnits.Kilometers) * 1000;
						
						// Sort routes by route number for this stop
						var sortedRoutes = routesForStop.OrderBy(r => r.RouteNumber).ToList();
						
						// Create and add a StopGroup object
						var stopGroup = new StopGroup(
							stop, 
							sortedRoutes, 
							distanceInMeters);
						
						stopGroups.Add(stopGroup);
						totalRoutes += routesForStop.Count;
					}
				}
				
				// Sort all stop groups by distance
				stopGroups = stopGroups.OrderBy(sg => sg.DistanceInMeters).ToList();
				
				// Create batch for UI updates to reduce UI thread load
				const int batchSize = 10;
				
				// First batch: Add the first batchSize stop groups with ETAs
				await Task.Run(async () => {
					// Prepare the first batch with ETAs
					for (int i = 0; i < Math.Min(batchSize, stopGroups.Count); i++)
					{
						await FetchAndApplyEtasForStopGroup(stopGroups[i]);
					}
					
					// Add first batch of stop groups - but only include stop groups with at least one route that has an ETA
					for (int i = 0; i < Math.Min(batchSize, stopGroups.Count); i++)
					{
						// Only add stop groups that have at least one route with a valid ETA
						if (stopGroups[i].HasAnyVisibleRoutesWithEta)
						{
							filteredItems.Add(stopGroups[i]);
						}
					}
					
					// Process remaining items in batches
					for (int i = batchSize; i < stopGroups.Count; i += batchSize)
					{
						var currentBatch = stopGroups.Skip(i).Take(batchSize).ToList();
						
						// Fetch ETAs for this batch in parallel
						var fetchTasks = currentBatch.Select(sg => FetchAndApplyEtasForStopGroup(sg));
						
						// Add current batch - but only include stop groups with at least one route that has an ETA
						foreach (var group in currentBatch)
						{
							// Only add stop groups that have at least one route with a valid ETA
							if (group.HasAnyVisibleRoutesWithEta)
							{
								filteredItems.Add(group);
							}
						}
						
						// Small delay between batches to keep UI responsive
						await Task.Delay(50);
					}
				});
				
				if (filteredItems.Count == 0) // Check filteredItems (StopGroups) count
				{
					noRoutesText = $"No routes found within {filter.ToString().Replace("Meters", "")} of your location.";
				}
				
				_logger?.LogDebug("Found {0} routes from {1} stops near current location within {2}",
					totalRoutes, filteredItems.Count, filter.ToString().Replace("Meters", ""));
			}
			
			// End batch operation to allow UI update
			filteredItems.EndBatch();
		}

		// Cache the results if no search query is applied
		if (string.IsNullOrEmpty(_searchQuery))
		{
			_stopGroupsCache[filter] = filteredItems;
		}
		
		// Update UI on the main thread - all at once to avoid multiple renders
		await MainThread.InvokeOnMainThreadAsync(() => 
		{
			try 
			{
				// Find label by name rather than directly referencing it
				var noRoutesLabel = this.FindByName<Microsoft.Maui.Controls.Label>("NoRoutesLabel");
				
				if (noRoutesLabel != null)
				{
					if (string.IsNullOrEmpty(_searchQuery) == false && filteredItems.Count == 0)
					{
						noRoutesLabel.Text = $"No routes matching '{_searchQuery}' found within {filter.ToString().Replace("Meters", "")} of your location.";
					}
					else if (filteredItems.Count == 0)
					{
						noRoutesLabel.Text = $"No stops found within {filter.ToString().Replace("Meters", "")} of your location.";
					}
				}
				
				// Optimize collection view rendering when large data is displayed
				var performanceOptimizer = Handler?.MauiContext?.Services?.GetService<PerformanceOptimizer>();
				performanceOptimizer?.OptimizeForLargeDataset(true);
				
				// Store current filter as previous for next update
				_previousDistanceFilter = filter;
			}
			catch (Exception ex)
			{
				_logger?.LogError(ex, "Error updating UI after distance filter: {message}", ex.Message);
			}
		});
		
		return filteredItems;
	}
	catch (Exception ex)
	{
		_logger?.LogError(ex, "Error in ApplyDistanceFilterAsync: {message}", ex.Message);
		
		// Ensure we still show an alert even if the UI update fails
		await DisplayAlert("Error", "Could not apply distance filter.", "OK");
		
		// Return empty collection on error
		return new ObservableRangeCollection<object>();
	}
}

/// <summary>
/// Incrementally updates stop groups when switching between distance filters
/// </summary>
private async Task<ObservableRangeCollection<object>> IncrementallyUpdateStopGroups(DistanceFilter previousFilter, DistanceFilter newFilter)
{
	// Determine if we're expanding or narrowing the range
	bool isExpandingRange = (int)newFilter > (int)previousFilter;
	
	// Safety check: If cache doesn't have previous filter entry, do full processing
	if (!_stopGroupsCache.ContainsKey(previousFilter))
	{
		_logger?.LogWarning("Cache doesn't have entry for {previousFilter}, returning empty collection", previousFilter);
		return new ObservableRangeCollection<object>();
	}
	
	// Use the existing cached collection as the starting point
	var existingItems = _stopGroupsCache[previousFilter];
	ObservableRangeCollection<object> result;
	
	if (isExpandingRange)
	{
		// Fix key format - use "100m", "200m", etc. instead of "100", "200", etc.
		string newFilterKey = newFilter.ToString().Replace("Meters", "") + "m";
		string previousFilterKey = previousFilter.ToString().Replace("Meters", "") + "m";
		
		// Check if stopsNearby contains required entries
		if (!_stopsNearby.ContainsKey(newFilterKey) || 
			!_stopsNearby.ContainsKey(previousFilterKey))
		{
			_logger?.LogWarning("Missing required stopsNearby entries, returning current results. Needed keys: {0} and {1}", 
				newFilterKey, previousFilterKey);
			return existingItems;
		}

		// If expanding, we need to add additional stops from the wider range
		List<TransportStop> additionalStops = new List<TransportStop>();
		
		// Get stops that are in the new range but not in the previous range
		switch (newFilter)
		{
			case DistanceFilter.Meters200 when previousFilter == DistanceFilter.Meters100:
				// Get stops between 100m and 200m
				additionalStops = _stopsNearby.ContainsKey("200m") && _stopsNearby.ContainsKey("100m") ? 
					_stopsNearby["200m"].Where(s => 
						!_stopsNearby["100m"].Any(s100 => s100.Id == s.Id)).ToList() : 
					new List<TransportStop>();
				break;
			case DistanceFilter.Meters400 when previousFilter == DistanceFilter.Meters200:
				// Get stops between 200m and 400m
				additionalStops = _stopsNearby.ContainsKey("400m") && _stopsNearby.ContainsKey("200m") ? 
					_stopsNearby["400m"].Where(s => 
						!_stopsNearby["200m"].Any(s200 => s200.Id == s.Id)).ToList() : 
					new List<TransportStop>();
				break;
			case DistanceFilter.Meters400 when previousFilter == DistanceFilter.Meters100:
				// Get stops between 100m and 400m
				additionalStops = _stopsNearby.ContainsKey("400m") && _stopsNearby.ContainsKey("100m") ? 
					_stopsNearby["400m"].Where(s => 
						!_stopsNearby["100m"].Any(s100 => s100.Id == s.Id)).ToList() : 
					new List<TransportStop>();
				break;
			case DistanceFilter.Meters600 when previousFilter == DistanceFilter.Meters400:
				// Get stops between 400m and 600m
				additionalStops = _stopsNearby.ContainsKey("600m") && _stopsNearby.ContainsKey("400m") ? 
					_stopsNearby["600m"].Where(s => 
						!_stopsNearby["400m"].Any(s400 => s400.Id == s.Id)).ToList() : 
					new List<TransportStop>();
				break;
			case DistanceFilter.Meters600 when previousFilter == DistanceFilter.Meters200:
				// Get stops between 200m and 600m
				additionalStops = _stopsNearby.ContainsKey("600m") && _stopsNearby.ContainsKey("200m") ? 
					_stopsNearby["600m"].Where(s => 
						!_stopsNearby["200m"].Any(s200 => s200.Id == s.Id)).ToList() : 
					new List<TransportStop>();
				break;
			case DistanceFilter.Meters600 when previousFilter == DistanceFilter.Meters100:
				// Get stops between 100m and 600m
				additionalStops = _stopsNearby.ContainsKey("600m") && _stopsNearby.ContainsKey("100m") ? 
					_stopsNearby["600m"].Where(s => 
						!_stopsNearby["100m"].Any(s100 => s100.Id == s.Id)).ToList() : 
					new List<TransportStop>();
				break;
			default:
				additionalStops = new List<TransportStop>();
				break;
		}
		
		// Process the additional stops
		if (additionalStops.Count > 0)
		{
			// Dictionary to group routes by stop
			var routesByStop = new Dictionary<string, Tuple<TransportStop, List<TransportRoute>>>();
			
			// Process each additional stop
			foreach (var stop in additionalStops)
			{
				// Get routes for this stop from database
				var routesForStop = await _databaseService.GetRoutesForStopAsync(stop.Id);
				
				// Apply text search filter if needed
				if (!string.IsNullOrEmpty(_searchQuery))
				{
					routesForStop = routesForStop.Where(r => 
						r.RouteNumber.Contains(_searchQuery, StringComparison.OrdinalIgnoreCase)).ToList();
				}
				
				// If this stop has matching routes, add it to our dictionary
				if (routesForStop.Any())
				{
					// Calculate distance from user to this stop
					var stopLocation = new Location(stop.Latitude, stop.Longitude);
					double distanceInMeters = Location.CalculateDistance(_userLocation, stopLocation, DistanceUnits.Kilometers) * 1000;
					
					// Sort routes by route number for this stop
					var sortedRoutes = routesForStop.OrderBy(r => r.RouteNumber).ToList();
					
					// Add stop to dictionary with distance as key for sorting
					string key = $"{distanceInMeters:0000.0}_{stop.Id}"; // Format to ensure proper sorting
					routesByStop[key] = new Tuple<TransportStop, List<TransportRoute>>(stop, sortedRoutes);
				}
			}
			
			// Create new StopGroup objects
			var newGroups = new List<StopGroup>();
			
			foreach (var key in routesByStop.Keys.OrderBy(k => k))
			{
				var stopData = routesByStop[key];
				var stop = stopData.Item1;
				var routesForStop = stopData.Item2;
				
				// Calculate stop distance for display
				var stopLocation = new Location(stop.Latitude, stop.Longitude);
				double distanceInMeters = Location.CalculateDistance(_userLocation, stopLocation, DistanceUnits.Kilometers) * 1000;
				
				// Create a StopGroup object
				var stopGroup = new StopGroup(
					stop, 
					routesForStop, 
					distanceInMeters);
				
				// Fetch ETAs for the new stop group
				try
				{
					_logger?.LogDebug("Fetching ETAs for incremental stop {stopId} ({distance:0.0}m)", stop.StopId, distanceInMeters);
					await FetchAndApplyEtasForStopGroup(stopGroup);
					
					// Add the stop group regardless of whether it has ETAs
					newGroups.Add(stopGroup);
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error fetching ETAs for incremental stop {stopId}, adding anyway", stop.StopId);
					newGroups.Add(stopGroup);
				}
			}
			
			// Now merge the new groups with the existing collection, maintaining distance order
			var allGroups = existingItems.OfType<StopGroup>().Concat(newGroups).OrderBy(g => g.DistanceInMeters).ToList();
			
			// Use optimized method to create the collection with batching
			result = new ObservableRangeCollection<object>();
			result.BeginBatch(); // Start batch to suppress notifications
			result.AddRange(allGroups); // Add all items at once
			result.EndBatch(); // End batch to allow UI update
		}
		else
		{
			// No new stops to add, return existing collection
			result = existingItems;
		}
		
		_logger?.LogDebug("Incrementally expanded from {0} to {1}, added {2} new stop groups",
			previousFilter, newFilter, result.Count - existingItems.Count);
	}
	else
	{
		// If narrowing, we need to remove stops that are outside the new range
		var stopsToKeep = new List<TransportStop>();
		
		// Safely get the list of stops to keep based on the new filter
		string distanceKey = "";
		switch (newFilter)
		{
			case DistanceFilter.Meters100:
				distanceKey = "100m";
				break;
			case DistanceFilter.Meters200:
				distanceKey = "200m";
				break;
			case DistanceFilter.Meters400:
				distanceKey = "400m";
				break;
			case DistanceFilter.Meters600:
				distanceKey = "600m";
				break;
		}
		
		// Check if we have data for this distance
		if (!string.IsNullOrEmpty(distanceKey) && _stopsNearby.ContainsKey(distanceKey))
		{
			stopsToKeep = _stopsNearby[distanceKey];
		}
		else
		{
			_logger?.LogWarning("Missing stops data for {distanceKey}, returning empty result", distanceKey);
			return new ObservableRangeCollection<object>();
		}
		
		// Create a new collection with only the stops in the narrower range
		var stopsToKeepIds = stopsToKeep.Select(s => s.Id).ToHashSet();
		
		var filteredGroups = existingItems.OfType<StopGroup>()
			.Where(sg => stopsToKeepIds.Contains(sg.Stop.Id))
			.ToList();
		
		// Use optimized method to create the collection with batching
		result = new ObservableRangeCollection<object>();
		result.BeginBatch(); // Start batch to suppress notifications
		result.AddRange(filteredGroups); // Add all items at once
		result.EndBatch(); // End batch to allow UI update
		
		_logger?.LogDebug("Incrementally narrowed from {0} to {1}, removed {2} stop groups",
			previousFilter, newFilter, existingItems.Count - result.Count);
	}
	
	// Cache the results if no search query is applied
	if (string.IsNullOrEmpty(_searchQuery))
	{
		_stopGroupsCache[newFilter] = result;
	}
	
	return result;
}

// Create a timer if it doesn't exist
if (_locationUpdateTimer == null)
{
	_locationUpdateTimer = Dispatcher.CreateTimer();
	// Use the constant value for consistency
	_locationUpdateTimer.Interval = TimeSpan.FromSeconds(LocationRefreshIntervalSeconds);
	_locationUpdateTimer.Tick += LocationUpdateTimer_Tick;
	
	_logger?.LogInformation("Created location update timer with interval {0} seconds", _locationUpdateTimer.Interval.TotalSeconds);
} 