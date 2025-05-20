using System.Collections.ObjectModel;
using System.Windows.Input;
using hk_realtime_transport_info_maui.Models;
using hk_realtime_transport_info_maui.Resources;
using hk_realtime_transport_info_maui.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Maui.Controls.Xaml;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Maui.Controls.PlatformConfiguration.WindowsSpecific;
using Microsoft.Maui.Controls.PlatformConfiguration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Maui.Devices;
using System.Globalization;
using NGeoHash;
using Microsoft.Maui.ApplicationModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using Microsoft.Maui.Controls;
using System;
using System.Diagnostics;
using System.Collections.Specialized;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;

namespace hk_realtime_transport_info_maui;

// StopGroup class has been moved to StopGroup.cs

// DataTemplateSelector for the CollectionView
public class MainPageDataTemplateSelector : DataTemplateSelector
{
	public required DataTemplate StopGroupTemplate { get; set; }
	public required DataTemplate RouteTemplate { get; set; }

	protected override DataTemplate OnSelectTemplate(object item, BindableObject container)
	{
		return item switch
		{
			StopGroup _ => StopGroupTemplate,
			TransportRoute _ => RouteTemplate,
			_ => throw new ArgumentOutOfRangeException(nameof(item), "Unknown item type for DataTemplateSelector")
		};
	}
}

// Separate class for AlphabetKeyModel
public class AlphabetKeyModel
{
	public string Key { get; set; } = string.Empty;
	public bool IsEnabled { get; set; }
	public Color BackgroundColor => IsEnabled ? Colors.White : Colors.LightGray;
	public Color TextColor => IsEnabled ? Colors.Black : Colors.Gray;
	
	// Add KeyboardManager property to fix the binding error
	public CustomKeyboardManager? KeyboardManager { get; set; }
	
	public AlphabetKeyModel(string key, bool isEnabled)
	{
		Key = key;
		IsEnabled = isEnabled;
	}
}

// Separate class for custom keyboard management
public class CustomKeyboardManager : BindableObject
{
	private bool _isKeyboardVisible;
	private Dictionary<string, bool> _enabledKeys = new Dictionary<string, bool>();
	private HashSet<string> _availableRouteChars = new HashSet<string>();
	private ObservableCollection<AlphabetKeyModel> _alphabetKeys = new ObservableCollection<AlphabetKeyModel>();
	
	// Cache for all characters present in any route number
	private HashSet<string> _globallyAvailableRouteChars = new HashSet<string>();
	private bool _globallyAvailableRouteCharsCalculated = false;
	
	// Calculated button dimensions
	private double _calculatedNumberButtonWidth;
	private double _calculatedNumberButtonHeight;
	private double _calculatedAlphabetButtonWidth;
	private double _calculatedAlphabetButtonHeight;
	private double _calculatedKeyboardHeight;

	public ObservableCollection<AlphabetKeyModel> AvailableAlphabetKeys
	{
		get => _alphabetKeys;
		set
		{
			_alphabetKeys = value;
			OnPropertyChanged();
		}
	}
	
	public bool IsKeyboardVisible 
	{ 
		get => _isKeyboardVisible; 
		set 
		{ 
			if (_isKeyboardVisible != value)
			{
				_isKeyboardVisible = value;
				OnPropertyChanged();
			}
		} 
	}
	
	// Number key enable properties
	public bool IsKey0Enabled => GetKeyEnabled("0");
	public bool IsKey1Enabled => GetKeyEnabled("1");
	public bool IsKey2Enabled => GetKeyEnabled("2");
	public bool IsKey3Enabled => GetKeyEnabled("3");
	public bool IsKey4Enabled => GetKeyEnabled("4");
	public bool IsKey5Enabled => GetKeyEnabled("5");
	public bool IsKey6Enabled => GetKeyEnabled("6");
	public bool IsKey7Enabled => GetKeyEnabled("7");
	public bool IsKey8Enabled => GetKeyEnabled("8");
	public bool IsKey9Enabled => GetKeyEnabled("9");
	
	// Number key background colors
	public Color Key0BackgroundColor => GetKeyBackgroundColor("0");
	public Color Key1BackgroundColor => GetKeyBackgroundColor("1");
	public Color Key2BackgroundColor => GetKeyBackgroundColor("2");
	public Color Key3BackgroundColor => GetKeyBackgroundColor("3");
	public Color Key4BackgroundColor => GetKeyBackgroundColor("4");
	public Color Key5BackgroundColor => GetKeyBackgroundColor("5");
	public Color Key6BackgroundColor => GetKeyBackgroundColor("6");
	public Color Key7BackgroundColor => GetKeyBackgroundColor("7");
	public Color Key8BackgroundColor => GetKeyBackgroundColor("8");
	public Color Key9BackgroundColor => GetKeyBackgroundColor("9");
	
	// Number key text colors
	public Color Key0TextColor => GetKeyTextColor("0");
	public Color Key1TextColor => GetKeyTextColor("1");
	public Color Key2TextColor => GetKeyTextColor("2");
	public Color Key3TextColor => GetKeyTextColor("3");
	public Color Key4TextColor => GetKeyTextColor("4");
	public Color Key5TextColor => GetKeyTextColor("5");
	public Color Key6TextColor => GetKeyTextColor("6");
	public Color Key7TextColor => GetKeyTextColor("7");
	public Color Key8TextColor => GetKeyTextColor("8");
	public Color Key9TextColor => GetKeyTextColor("9");
	
	// Button dimensions properties
	public double CalculatedNumberButtonWidth 
	{
		get => _calculatedNumberButtonWidth;
		set
		{
			if (_calculatedNumberButtonWidth != value)
			{
				_calculatedNumberButtonWidth = value;
				OnPropertyChanged();
			}
		}
	}
	
	public double CalculatedNumberButtonHeight
	{
		get => _calculatedNumberButtonHeight;
		set
		{
			if (_calculatedNumberButtonHeight != value)
			{
				_calculatedNumberButtonHeight = value;
				OnPropertyChanged();
			}
		}
	}
	
	public double CalculatedAlphabetButtonWidth
	{
		get => _calculatedAlphabetButtonWidth;
		set
		{
			if (_calculatedAlphabetButtonWidth != value)
			{
				_calculatedAlphabetButtonWidth = value;
				OnPropertyChanged();
			}
		}
	}
	
	public double CalculatedAlphabetButtonHeight
	{
		get => _calculatedAlphabetButtonHeight;
		set
		{
			if (_calculatedAlphabetButtonHeight != value)
			{
				_calculatedAlphabetButtonHeight = value;
				OnPropertyChanged();
			}
		}
	}
	
	public double CalculatedKeyboardHeight
	{
		get => _calculatedKeyboardHeight;
		set
		{
			if (_calculatedKeyboardHeight != value)
			{
				_calculatedKeyboardHeight = value;
				OnPropertyChanged();
			}
		}
	}
	
	public CustomKeyboardManager()
	{
		InitializeKeyboardState();
	}
	
	public void InitializeKeyboardState()
	{
		foreach (char digit in "0123456789")
		{
			_enabledKeys[digit.ToString()] = false;
		}
		
		foreach (char c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		{
			_enabledKeys[c.ToString()] = false;
		}
		
		UpdateAlphabetKeys();
	}
	
	public void PrecomputeGloballyAvailableRouteChars(List<TransportRoute> allMasterRoutes)
	{
		_globallyAvailableRouteChars.Clear();
		if (allMasterRoutes == null)
		{
			_globallyAvailableRouteCharsCalculated = false;
			return;
		}
		foreach (var route in allMasterRoutes)
		{
			if (route != null && !string.IsNullOrEmpty(route.RouteNumber))
			{
				foreach (char c in route.RouteNumber)
				{
					_globallyAvailableRouteChars.Add(c.ToString().ToUpperInvariant());
				}
			}
		}
		_globallyAvailableRouteCharsCalculated = true;
	}
	
	public void UpdateAvailableRouteChars(List<TransportRoute> filteredRoutes, string searchQuery)
	{
		_availableRouteChars.Clear();
		
		if (string.IsNullOrEmpty(searchQuery))
		{
			// If no search query, use precomputed global set if available
			if (_globallyAvailableRouteCharsCalculated)
			{
				// Directly use the globally available characters
				// No need to union, just assign the set's content or use it directly for checks.
				// For simplicity in current structure, we'll populate _availableRouteChars from it.
				foreach(var c in _globallyAvailableRouteChars)
				{
					_availableRouteChars.Add(c);
				}
			}
			else
			{
				// Fallback: If precomputation hasn't happened (should not occur in normal flow after init)
				// or if filteredRoutes is the only source (e.g. initial load before _allRoutes is ready for precomputation)
				if (filteredRoutes != null)
				{
					foreach (var route in filteredRoutes)
					{
						if (route != null && !string.IsNullOrEmpty(route.RouteNumber))
						{
							foreach (char c in route.RouteNumber)
							{
								_availableRouteChars.Add(c.ToString().ToUpperInvariant());
							}
						}
					}
				}
			}
		}
		else
		{
			// If we have a search query, only include chars that would lead to valid route numbers
			if (filteredRoutes != null)
			{
				foreach (var route in filteredRoutes)
				{
					if (route == null || string.IsNullOrEmpty(route.RouteNumber)) continue;

					string routeNum = route.RouteNumber;
					
					// Only consider this route if it starts with our search query
					if (routeNum.StartsWith(searchQuery, StringComparison.OrdinalIgnoreCase))
					{
						// Only add the next character after our search query
						if (searchQuery.Length < routeNum.Length)
						{
							_availableRouteChars.Add(routeNum[searchQuery.Length].ToString().ToUpperInvariant());
						}
					}
				}
			}
		}
		
		// Update the enabled state of each key
		foreach (char digit in "0123456789")
		{
			string digitStr = digit.ToString();
			_enabledKeys[digitStr] = _availableRouteChars.Contains(digitStr);
		}
		
		foreach (char c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		{
			string key = c.ToString();
			_enabledKeys[key] = _availableRouteChars.Contains(key);
		}
		
		// Update the alphabet keys collection
		UpdateAlphabetKeys();
		
		// Notify all key properties have changed
		UpdateKeyProperties();
	}
	
	private void UpdateAlphabetKeys()
	{
		// Create alphabet keys (uppercase only)
		_alphabetKeys.Clear();
		
		// Create list of capital letters and numbers
		var allChars = new List<string>();
		
		// Letters A-Z
		for (char c = 'A'; c <= 'Z'; c++)
		{
			allChars.Add(c.ToString());
		}
		
		foreach (var key in allChars)
		{
			bool isEnabled = _availableRouteChars.Contains(key);
			var keyModel = new AlphabetKeyModel(key, isEnabled)
			{
				KeyboardManager = this  // Set reference to keyboard manager
			};
			_alphabetKeys.Add(keyModel);
		}
		
		// Notify that available alphabet keys have changed
		OnPropertyChanged(nameof(AvailableAlphabetKeys));
	}
	
	private bool GetKeyEnabled(string key)
	{
		return _enabledKeys.ContainsKey(key) && _enabledKeys[key];
	}
	
	private Color GetKeyBackgroundColor(string key)
	{
		return GetKeyEnabled(key) ? Colors.White : Colors.LightGray;
	}
	
	private Color GetKeyTextColor(string key)
	{
		return GetKeyEnabled(key) ? Colors.Black : Colors.Gray;
	}
	
	private void UpdateKeyProperties()
	{
		OnPropertyChanged(nameof(IsKey0Enabled));
		OnPropertyChanged(nameof(IsKey1Enabled));
		OnPropertyChanged(nameof(IsKey2Enabled));
		OnPropertyChanged(nameof(IsKey3Enabled));
		OnPropertyChanged(nameof(IsKey4Enabled));
		OnPropertyChanged(nameof(IsKey5Enabled));
		OnPropertyChanged(nameof(IsKey6Enabled));
		OnPropertyChanged(nameof(IsKey7Enabled));
		OnPropertyChanged(nameof(IsKey8Enabled));
		OnPropertyChanged(nameof(IsKey9Enabled));
		
		OnPropertyChanged(nameof(Key0BackgroundColor));
		OnPropertyChanged(nameof(Key1BackgroundColor));
		OnPropertyChanged(nameof(Key2BackgroundColor));
		OnPropertyChanged(nameof(Key3BackgroundColor));
		OnPropertyChanged(nameof(Key4BackgroundColor));
		OnPropertyChanged(nameof(Key5BackgroundColor));
		OnPropertyChanged(nameof(Key6BackgroundColor));
		OnPropertyChanged(nameof(Key7BackgroundColor));
		OnPropertyChanged(nameof(Key8BackgroundColor));
		OnPropertyChanged(nameof(Key9BackgroundColor));
		
		OnPropertyChanged(nameof(Key0TextColor));
		OnPropertyChanged(nameof(Key1TextColor));
		OnPropertyChanged(nameof(Key2TextColor));
		OnPropertyChanged(nameof(Key3TextColor));
		OnPropertyChanged(nameof(Key4TextColor));
		OnPropertyChanged(nameof(Key5TextColor));
		OnPropertyChanged(nameof(Key6TextColor));
		OnPropertyChanged(nameof(Key7TextColor));
		OnPropertyChanged(nameof(Key8TextColor));
		OnPropertyChanged(nameof(Key9TextColor));
	}
	
	public void UpdateKeyboardButtonWidths(double width)
	{
		double screenWidth = width;
		double screenHeight = DeviceDisplay.MainDisplayInfo.Height / DeviceDisplay.MainDisplayInfo.Density;
		
		// Set keyboard height to 25% of screen height (even more compact)
		CalculatedKeyboardHeight = screenHeight * 0.25;
		
		// Determine button sizes based on screen width
		// Number buttons are in a 3-column grid
		CalculatedNumberButtonWidth = (screenWidth - 40) / 3;  // 40 padding
		CalculatedNumberButtonHeight = CalculatedNumberButtonWidth * 0.50;
		
		// Alphabet buttons are in a 2-column grid
		CalculatedAlphabetButtonWidth = (screenWidth - 40) / 6;  // 40 padding
		CalculatedAlphabetButtonHeight = CalculatedAlphabetButtonWidth * 0.8;
	}
}

public class RouteTrie
{
	private readonly TrieNode _root;
	
	public RouteTrie()
	{
		_root = new TrieNode();
	}
	
	public void AddRoute(TransportRoute route)
	{
		string routeNumber = route.RouteNumber.ToUpperInvariant();
		TrieNode current = _root;
		
		foreach (char c in routeNumber)
		{
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
			if (!current.Children.TryGetValue(c, out TrieNode child))
			{
				child = new TrieNode();
				current.Children[c] = child;
			}
#pragma warning restore CS8600
			
			current = child;
			current.Routes.Add(route);
		}
		
		current.IsEndOfWord = true;
	}
	
	public List<TransportRoute> Search(string prefix)
	{
		if (string.IsNullOrEmpty(prefix))
		{
			return _root.GetAllRoutes();
		}
		
		prefix = prefix.ToUpperInvariant();
		TrieNode current = _root;
		
		foreach (char c in prefix)
		{
			if (!current.Children.TryGetValue(c, out TrieNode? child) || child == null)
			{
				return new List<TransportRoute>();
			}
			
			current = child!; // Add null-forgiving operator here
		}
		
		return current.Routes.ToList();
	}
	
	public HashSet<string> GetNextPossibleChars(string prefix)
	{
		var result = new HashSet<string>();
		
		if (string.IsNullOrEmpty(prefix))
		{
			foreach (var kvp in _root.Children)
			{
				string keyStr = kvp.Key.ToString() ?? string.Empty;
				result.Add(keyStr);
			}
			return result;
		}
		
		prefix = prefix.ToUpperInvariant();
		TrieNode current = _root;
		
		foreach (char c in prefix)
		{
			if (!current.Children.TryGetValue(c, out TrieNode? child) || child == null)
			{
				return result;
			}
			
			current = child!; // Add null-forgiving operator
		}
		
		foreach (var kvp in current.Children)
		{
			string keyStr = kvp.Key.ToString() ?? string.Empty;
			result.Add(keyStr);
		}
		
		return result;
	}
	
	public void Clear()
	{
		_root.Children.Clear();
	}
	
	private class TrieNode
	{
		public Dictionary<char, TrieNode> Children { get; }
		public HashSet<TransportRoute> Routes { get; }
		public bool IsEndOfWord { get; set; }
		
		public TrieNode()
		{
			Children = new Dictionary<char, TrieNode>();
			Routes = new HashSet<TransportRoute>();
			IsEndOfWord = false;
		}
		
		public List<TransportRoute> GetAllRoutes()
		{
			var result = new HashSet<TransportRoute>(Routes);
			
			foreach (var child in Children.Values)
			{
				foreach (var route in child.GetAllRoutes())
				{
					result.Add(route);
				}
			}
			
			return result.ToList();
		}
	}
}

public partial class MainPage : ContentPage
{
	private readonly LiteDbService _databaseService;
	private readonly KmbDataService _kmbDataService;
	private readonly MtrDataService _mtrDataService;
	private readonly EtaService _etaService;
	private readonly ILogger<MainPage> _logger;
	private readonly LocationCacheService _locationCacheService; // Added LocationCacheService
	private ObservableRangeCollection<object> _routes = new(); // Use object for mixed types
	private ObservableRangeCollection<object> _filteredRoutes = new(); // Use object for mixed types
	private List<TransportRoute> _allRoutes = new();
	private Dictionary<string, TransportRoute> _allRoutesDict = new(); // Add this for O(1) lookups
	private RouteTrie _routeTrie = new RouteTrie(); // Add route trie for optimized searching
	private Dictionary<string, List<TransportStop>> _cachedRawStopsByCenterGeoHash6 = new(); // NEW CACHE
	private bool _isRefreshing;
	private bool _isDataLoading = true;
	private double _downloadProgress;
	private string _downloadStatusMessage = "Loading...";
	private readonly SemaphoreSlim _loadingSemaphore = new SemaphoreSlim(1, 1);
	private bool _isHidingIndicator;
	private string _searchQuery = string.Empty;
	
	// private ObservableCollection<object> _allRoutesItems = new(); // This remains as a cache for FilterRoutesInternal
	private List<object>? _allRoutesItemsCache; // Cache for all items when no search query.
	private bool _isNearbyViewVisible = true;
	private bool _isAllViewVisible = false;
	
	// Debouncing support
	private CancellationTokenSource? _filterCancellationTokenSource;
	private readonly object _filterLock = new object();
	private const int FilterDebounceMilliseconds = 150;
	
	// Custom keyboard management
	public CustomKeyboardManager KeyboardManager { get; } = new CustomKeyboardManager();
	
	// Property to check if there is search text
	public bool HasSearchText => !string.IsNullOrEmpty(_searchQuery);
	
	// Clear button styling
	public Color ClearButtonBackgroundColor => HasSearchText ? Colors.White : Colors.LightGray;
	public Color ClearButtonTextColor => HasSearchText ? Colors.Black : Colors.Gray;
	
	// Delete button styling
	public Color DeleteButtonBackgroundColor => HasSearchText ? Colors.White : Colors.LightGray;
	public Color DeleteButtonTextColor => HasSearchText ? Colors.Black : Colors.Gray;
	
	private double _lastWidth;

	// Add a cache for stop groups by distance
	private Dictionary<DistanceFilter, ObservableRangeCollection<object>> _stopGroupsCache = new Dictionary<DistanceFilter, ObservableRangeCollection<object>>();
	private DistanceFilter _previousDistanceFilter = DistanceFilter.All;

	public ObservableRangeCollection<object> Routes 
	{ 
		get => _filteredRoutes;
		private set
		{
			if (_filteredRoutes != value)
			{
				// Detect if this is a call from UpdateEtasForCurrentView or UpdateExpandedStopGroupEtas
				bool isEtaUpdate = new StackTrace().ToString().Contains("UpdateEtas");
				
				// Check if the collection is the same instance but different content
				bool sameInstance = ReferenceEquals(_filteredRoutes, value);
				
				// During ETA updates, we prefer modifying the existing collection in-place
				// instead of replacing it to avoid scroll position reset
				if (isEtaUpdate && !sameInstance && _filteredRoutes != null && _filteredRoutes.Count > 0)
				{
					_logger?.LogDebug("Routes setter: ETA update detected, preserving existing collection to maintain scroll position");
					
					// Just notify that the content changed but don't replace the collection
					OnPropertyChanged(nameof(Routes));
					return;
				}
				
				_filteredRoutes = value;
				OnPropertyChanged(nameof(Routes));
				
				// Update available characters for keyboard when routes change
				if (_filteredRoutes != null)
				{
					// Only update keyboard on search-related changes, not during distance filtering
					bool isDistanceFilterOperation = new StackTrace().ToString().Contains("ApplyDistanceFilterAsync") ||
						false;
						
					if (!isDistanceFilterOperation)
					{
						// Filter only TransportRoute items for the keyboard manager
						var actualRoutes = _filteredRoutes.OfType<TransportRoute>().ToList();
						KeyboardManager.UpdateAvailableRouteChars(actualRoutes, _searchQuery);
					}
				}
			}
		}
	}

	public bool IsNearbyViewVisible
	{
		get => _isNearbyViewVisible;
		set
		{
			if (_isNearbyViewVisible != value)
			{
				_isNearbyViewVisible = value;
				OnPropertyChanged(nameof(IsNearbyViewVisible));
			}
		}
	}

	public bool IsAllViewVisible
	{
		get => _isAllViewVisible;
		set
		{
			if (_isAllViewVisible != value)
			{
				_isAllViewVisible = value;
				OnPropertyChanged(nameof(IsAllViewVisible));
			}
		}
	}

	public bool IsRefreshing
	{
		get => _isRefreshing;
		set
		{
			_isRefreshing = value;
			OnPropertyChanged();
		}
	}

	public bool IsDataLoading
	{
		get => _isDataLoading;
		set
		{
			if (_isDataLoading != value)
			{
				_isDataLoading = value;
				OnPropertyChanged();
				_logger?.LogInformation("Data loading state changed to: {isLoading}", value);
				
				// When loading completes, ensure UI is refreshed
				if (!value)
				{
					MainThread.BeginInvokeOnMainThread(() => {
						// Force layout refresh to ensure loading indicator disappears
						this.ForceLayout();
					});
				}
			}
		}
	}

	public double DownloadProgress
	{
		get => _downloadProgress;
		set
		{
			_downloadProgress = value;
			OnPropertyChanged();
		}
	}

	public string DownloadStatusMessage
	{
		get => _downloadStatusMessage;
		set
		{
			_downloadStatusMessage = value;
			OnPropertyChanged();
		}
	}

	public ICommand RefreshCommand { get; }

	// Distance filter options
	private enum DistanceFilter
	{
		All,
		Meters100,
		Meters200,
		Meters400,
		Meters600
	}
	
	private DistanceFilter _currentDistanceFilter = DistanceFilter.Meters200; // Default to 200m
	private Dictionary<string, List<TransportStop>> _stopsNearby = new();
	private Location? _userLocation;
	private DateTime _lastLocationUpdate = DateTime.MinValue;
	private const int LocationRefreshIntervalSeconds = 60;
	
	// Button colors for distance filter
	private Color _selectedButtonColor = Color.FromArgb("#4B0082"); // Purple
	private Color _selectedTextColor = Colors.White;
	private Color _unselectedButtonColor = Colors.LightGray;
	private Color _unselectedTextColor = Colors.Black;
	
	public Color Distance100mButtonColor => _currentDistanceFilter == DistanceFilter.Meters100 ? _selectedButtonColor : _unselectedButtonColor;
	public Color Distance100mTextColor => _currentDistanceFilter == DistanceFilter.Meters100 ? _selectedTextColor : _unselectedTextColor;
	
	public Color Distance200mButtonColor => _currentDistanceFilter == DistanceFilter.Meters200 ? _selectedButtonColor : _unselectedButtonColor;
	public Color Distance200mTextColor => _currentDistanceFilter == DistanceFilter.Meters200 ? _selectedTextColor : _unselectedTextColor;
	
	public Color Distance400mButtonColor => _currentDistanceFilter == DistanceFilter.Meters400 ? _selectedButtonColor : _unselectedButtonColor;
	public Color Distance400mTextColor => _currentDistanceFilter == DistanceFilter.Meters400 ? _selectedTextColor : _unselectedTextColor;
	
	public Color Distance600mButtonColor => _currentDistanceFilter == DistanceFilter.Meters600 ? _selectedButtonColor : _unselectedButtonColor;
	public Color Distance600mTextColor => _currentDistanceFilter == DistanceFilter.Meters600 ? _selectedTextColor : _unselectedTextColor;
	
	public Color DistanceAllButtonColor => _currentDistanceFilter == DistanceFilter.All ? _selectedButtonColor : _unselectedButtonColor;
	public Color DistanceAllTextColor => _currentDistanceFilter == DistanceFilter.All ? _selectedTextColor : _unselectedTextColor;

	// Add a lock object for thread safety in ETA updates
	private readonly object _etaLock = new object();
	private bool _isUpdatingEtas = false;

	// Keep track of whether we have a timer running already
	private bool _etaTimerRunning = false;
	private readonly object _timerLock = new object();

	// Add a single timer field for all ETA updates
	private IDispatcherTimer? _etaUpdateTimer;
	// Add a timer for location updates
	private IDispatcherTimer? _locationUpdateTimer;

	// Field to cache all routes for better performance
	
	// Track the current loading batch
	private int _currentBatchIndex = 0;
	
	// Flag to track if initial UI update has been done
	private bool _initialUIUpdateDone = false;

	// Field to store the previous distance filter for incremental updates
	private DistanceFilter _previousDistanceFilterForIncrementalUpdate = DistanceFilter.Meters200;

	private readonly SemaphoreSlim _adaptLock = new SemaphoreSlim(1, 1);
	private bool _isUpdatingEtaDisplay = false;
	
	// Timer for ETA display text updates (updates display without fetching new ETAs)
	private IDispatcherTimer? _etaDisplayUpdateTimer;

	// Add these fields after _currentBatchIndex
	private int _lastScrolledItemIndex = 0;
	private int _lastScrolledItemOffset = 0;
	private bool _isRestoringScrollPosition = false;
	private bool _shouldRestoreScrollPosition = false;

	public MainPage(LiteDbService databaseService, KmbDataService kmbDataService, MtrDataService mtrDataService, EtaService etaService, 
		ILogger<MainPage> logger, LocationCacheService locationCacheService)
	{
		// Initialize the XAML components
		
		InitializeComponent();
		
		_databaseService = databaseService;
		_kmbDataService = kmbDataService;
		_mtrDataService = mtrDataService;
		_etaService = etaService;
		_logger = logger;
		_locationCacheService = locationCacheService; // Store the LocationCacheService
		
		// Initialize debouncing token source
		_filterCancellationTokenSource = new CancellationTokenSource();
		
		// Subscribe to progress events
		_kmbDataService.ProgressChanged += OnDownloadProgressChanged;
		_mtrDataService.ProgressChanged += OnDownloadProgressChanged;
		
		// Disable the built-in keyboard for the search bar
		RouteSearchBar.IsSpellCheckEnabled = false;
		RouteSearchBar.IsTextPredictionEnabled = false;
		
		// Initialize refresh command
		RefreshCommand = new Command(LoadRoutes);
		
		// Set the binding context before initializing data
		BindingContext = this;
		
		// Enable CollectionView optimizations
		if (this.FindByName<CollectionView>("RoutesCollection") is CollectionView collectionView)
		{
			// Enable virtualization
			collectionView.ItemsLayout = new LinearItemsLayout(ItemsLayoutOrientation.Vertical)
			{
				ItemSpacing = 5,
			};
			
			// Set items updating scroll mode to keep position
			collectionView.ItemsUpdatingScrollMode = ItemsUpdatingScrollMode.KeepScrollOffset;
			
			// Set a smaller RemainingItemsThreshold to reduce memory usage (moved from AllRoutesCollection block)
			collectionView.RemainingItemsThreshold = 5;
			
			// Add event handler for lazy loading (moved from AllRoutesCollection block)
			collectionView.RemainingItemsThresholdReached += OnAllRoutesRemainingItemsThresholdReached;
			
			// Disable constrained layout for better performance
			#if WINDOWS
			Microsoft.Maui.Controls.PlatformConfiguration.WindowsSpecific.CollectionView.SetConstrainLayout(
				collectionView, false);
			#endif
		}
		
		// Initialize keyboard related properties
		KeyboardManager.IsKeyboardVisible = false;
		
		// Initialize alphabet keys collection
		KeyboardManager.AvailableAlphabetKeys = new ObservableCollection<AlphabetKeyModel>();
		
		// Set title and other UI elements from code
		UpdateUIFromResources();
		
		// Initialize a new empty observable collection 
		Routes = new ObservableRangeCollection<object>();
		
		// Explicitly set up search handler to ensure it's connected
		RouteSearchBar.TextChanged += OnSearchTextChanged;
	}
	
	// Helper method to update UI elements from resources
	private void UpdateUIFromResources()
	{
		try
		{
			// Update page title
			Title = App.GetString("AppTitle", "Route Information");
			
			// Update SearchBar placeholder
			var searchBar = this.FindByName<Microsoft.Maui.Controls.SearchBar>("RouteSearchBar");
			if (searchBar != null)
			{
				searchBar.Placeholder = App.GetString("SearchRoutes", "Search routes...");
			}
			
			// Update No Routes label
			var noRoutesLabel = this.FindByName<Microsoft.Maui.Controls.Label>("NoRoutesLabel");
			if (noRoutesLabel != null)
			{
				noRoutesLabel.Text = App.GetString("NoRoutesFound", "No routes found.");
			}
			
			// Update Refresh button
			var refreshButton = this.FindByName<Button>("RefreshButton");
			if (refreshButton != null)
			{
				refreshButton.Text = App.GetString("Refresh", "Refresh");
			}
			
			_logger?.LogDebug("UI updated from string resources");
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error updating UI from resources");
		}
	}

	protected override async void OnAppearing()
	{
		base.OnAppearing();
		
		// Show UI immediately instead of waiting for data
		if (_allRoutes.Count == 0)
		{
			_logger?.LogInformation("OnAppearing: _allRoutes is empty, calling LoadRoutesDirectly.");
			await LoadRoutesDirectly(false); // Load data without showing refresh indicator
			// Another pause to let other database operations complete
			await Task.Delay(50);
		}
		
		// If we're showing the "All" view, initialize the Routes collection if it should contain all routes
		if (_currentDistanceFilter == DistanceFilter.All && Routes.Count == 0) // Check Routes.Count directly
		{
			// IMPORTANT: Avoid UI freezing by loading just a few routes initially
			var initialRoutes = _allRoutes.Take(50).Cast<object>().ToList();
			var newCollection = new ObservableRangeCollection<object>();
			newCollection.AddRange(initialRoutes);
			Routes = newCollection;
			
			// Then load the rest in the background 
			_ = Task.Run(async () => 
			{
				// Wait to ensure UI has updated
				await Task.Delay(100);
				await LoadAllRoutesWithVirtualization();
			});
		}
		else if (_currentDistanceFilter != DistanceFilter.All && Routes.Count == 0) // Check Routes.Count, assuming it should contain nearby items
		{
			// Initialize nearby routes for the current filter
			// UpdateUserLocationIfNeeded will call ApplyDistanceFilterAsync, which updates Routes.
			await UpdateUserLocationIfNeeded();
		}
		else
		{
			// Update location without changing the filter, potentially refreshing Routes if filter is active
			await UpdateUserLocationIfNeeded();
		}
		
			// Start location update timer
	StartLocationUpdateTimer();
	
	// Start the ETA display update timer
	StartEtaDisplayUpdateTimer();
	}
	
	protected override void OnDisappearing()
	{
		base.OnDisappearing();
		
		try
		{
			// Stop the ETA update timer
			lock (_timerLock)
			{
				_etaTimerRunning = false;
				if (_etaUpdateTimer != null && _etaUpdateTimer.IsRunning)
				{
					_etaUpdateTimer.Stop();
					_logger?.LogDebug("Stopped ETA update timer");
				}
			}
			
					// Stop the location update timer
		if (_locationUpdateTimer != null && _locationUpdateTimer.IsRunning)
		{
			_locationUpdateTimer.Stop();
			_logger?.LogDebug("Stopped location update timer");
		}
		
		// Stop the ETA display update timer
		if (_etaDisplayUpdateTimer != null && _etaDisplayUpdateTimer.IsRunning)
		{
			_etaDisplayUpdateTimer.Stop();
			_logger?.LogDebug("Stopped ETA display update timer");
		}
			
			// Remove scroll handler
			if (this.FindByName<CollectionView>("RoutesCollection") is CollectionView collectionView)
			{
				collectionView.Scrolled -= OnCollectionViewScrolled;
			}
			
			// Return animations to normal
			var performanceOptimizer = Handler?.MauiContext?.Services?.GetService<PerformanceOptimizer>();
			performanceOptimizer?.OptimizeForLargeDataset(false);
			
			// Unsubscribe from events
			if (_kmbDataService != null)
			{
				_kmbDataService.ProgressChanged -= OnDownloadProgressChanged;
			}
			
			App.UnregisterDataReadyCallback("MainPage");
			
			// Hide keyboard if visible
			KeyboardManager.IsKeyboardVisible = false;
			
			// Cleanup debounce token source
			lock (_filterLock)
			{
				if (_filterCancellationTokenSource != null)
				{
					try
					{
						if (!_filterCancellationTokenSource.IsCancellationRequested)
						{
							_filterCancellationTokenSource.Cancel();
						}
						_filterCancellationTokenSource.Dispose();
					}
					catch (ObjectDisposedException)
					{
						// Already disposed, ignore
					}
					catch (Exception ex)
					{
						_logger?.LogError(ex, "Error disposing CancellationTokenSource in OnDisappearing");
					}
					_filterCancellationTokenSource = null;
				}
			}
			
			// Clean up resources
			if (_loadingSemaphore != null)
			{
				_loadingSemaphore.Dispose();
			}
			
			_logger?.LogDebug("MainPage OnDisappearing");
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in OnDisappearing");
		}
	}
	
	private void OnDownloadProgressChanged(object? sender, DownloadProgressEventArgs e)
	{
		MainThread.BeginInvokeOnMainThread(() => 
		{
			DownloadProgress = e.Progress;
			DownloadStatusMessage = e.StatusMessage;
			
			// Check for the special completion message
			if (e.StatusMessage == "DOWNLOAD_COMPLETE")
			{
				_logger?.LogInformation("Received explicit download complete signal");
				HideLoadingIndicator();
				return;
			}
			
			// If progress is 100%, hide the loading indicator
			if (e.Progress >= 1.0)
			{
				HideLoadingIndicator();
			}
			
			// Check for completed message
			if (e.StatusMessage.Contains("completed", StringComparison.OrdinalIgnoreCase))
			{
				HideLoadingIndicator();
			}
		});
	}

	private async void HideLoadingIndicator()
	{
		// Use semaphore to prevent concurrent modifications and duplication
		if (_isHidingIndicator) return;
		
		try
		{
			// Set flag before waiting to prevent multiple attempts
			_isHidingIndicator = true;
			
			// Use TryWait instead of a blocking wait with timeout
			bool acquired = _loadingSemaphore.Wait(0);
			if (!acquired) return;
			
			try
			{
				// Only log once and only do work if the indicator is actually visible
				if (IsDataLoading)
				{
					_logger?.LogInformation("Hiding loading indicator");
					IsDataLoading = false;
					
					// Find the loading grid and explicitly hide it
					await MainThread.InvokeOnMainThreadAsync(() => 
					{
						var loadingGrid = this.FindByName<Grid>("LoadingGrid");
						if (loadingGrid != null && loadingGrid.IsVisible)
						{
							loadingGrid.IsVisible = false;
							_logger?.LogDebug("Hidden LoadingGrid directly");
						}
						
						// Force update layout
						this.ForceLayout();
					});
				}
			}
			finally
			{
				// Release the semaphore
				_loadingSemaphore.Release();
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error hiding loading indicator");
		}
		finally
		{
			_isHidingIndicator = false;
		}
	}

	private async void OnDataReady()
	{
		// Log at INFO level on UI thread
		MainThread.BeginInvokeOnMainThread(() =>
		{
			HideLoadingIndicator();
		});
		
		// Add this line to fetch and update ETAs after loading is done
		// Run in a background task to avoid blocking UI
		await Task.Run(async () => 
		{
			try
			{
				// Wait a bit to ensure database operations are complete
				await Task.Delay(2000);
				
				// Ensure we're still connected to the app
				await MainThread.InvokeOnMainThreadAsync(async () => 
				{
					_logger?.LogInformation("OnDataReady: Triggering ETA update for current view if applicable.");
					// Only update ETAs if we are in a nearby view and Routes has items
					if (_currentDistanceFilter != DistanceFilter.All && Routes != null && Routes.Any())
					{
						_logger?.LogInformation("OnDataReady: Current view has {count} items, proceeding with ETA update.", Routes.Count);
						var currentDisplayItems = new ObservableRangeCollection<object>(Routes.ToList()); // Pass a copy
						await UpdateEtasForCurrentView(currentDisplayItems);
					}
					else
					{
						_logger?.LogInformation("OnDataReady: Skipping ETA update. Filter is ALL or Routes is empty. Current Filter: {filter}, Routes Count: {count}", 
							_currentDistanceFilter, Routes?.Count ?? 0);
					}
				});
			}
			catch (Exception ex)
			{
				_logger?.LogError(ex, "Error starting ETA updates in OnDataReady");
			}
		});
	}
	
	private void LoadRoutes()
	{
		// Show loading indicators
		IsRefreshing = true;
		
		// Run in background
		Task.Run(async () => 
		{
			try
			{
				// Reload routes directly from database without forcing a data refresh
				await LoadRoutesDirectly(true);
			}
			finally
			{
				// Ensure we hide the loading indicator
				await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
			}
		});
	}
	
	// Method to handle language changes
	public void OnLanguageChanged()
	{
		// Update UI with new language resources
		UpdateUIFromResources();
		
		// Force a collection refresh to update bindings
		if (Routes.Count > 0)
		{
			var tempRoutes = new ObservableRangeCollection<object>(Routes);
			Routes = new ObservableRangeCollection<object>();
			Routes = tempRoutes;
			
			// Force refresh of CollectionView
			if (this.FindByName<CollectionView>("RoutesCollection") is CollectionView cv)
			{
				cv.ItemsSource = null;
				cv.ItemsSource = Routes;
			}
		}
	}

	// Handle search bar focus - show custom keyboard and prevent default keyboard
	private void OnSearchBarFocused(object sender, FocusEventArgs e)
	{
		if (e.IsFocused)
		{
			// Show custom keyboard
			KeyboardManager.IsKeyboardVisible = true;
			
			// Prevent the default keyboard from appearing
			MainThread.BeginInvokeOnMainThread(() => {
				// Hide the default keyboard
				if (DeviceInfo.Platform == DevicePlatform.iOS || DeviceInfo.Platform == DevicePlatform.MacCatalyst)
				{
					// For iOS, immediately unfocus and then re-focus to prevent default keyboard
					RouteSearchBar.Unfocus();
					RouteSearchBar.Focus();
				}
				// We don't need to add Android-specific code here as hiding soft keyboard is handled differently
			});
		}
	}
	
	// Handle search bar unfocus
	private void OnSearchBarUnfocused(object sender, FocusEventArgs e)
	{
		// Only hide the keyboard if we're explicitly unfocusing
		// and not just switching focus within the search process
		if (!e.IsFocused && !KeyboardManager.IsKeyboardVisible)
		{
			// Ensure we're not in the middle of a focus/unfocus cycle
			Dispatcher.DispatchDelayed(TimeSpan.FromMilliseconds(50), () =>
			{
				if (!RouteSearchBar.IsFocused)
				{
					MainThread.BeginInvokeOnMainThread(() => KeyboardManager.IsKeyboardVisible = false);
				}
			});
		}
	}
	
	// Handle taps on the background to hide keyboard
    private void OnBackgroundTapped(object sender, TappedEventArgs e)
    {
        if (KeyboardManager.IsKeyboardVisible)
        {
            KeyboardManager.IsKeyboardVisible = false;
            
            // Also clear focus from search bar
            if (RouteSearchBar != null && RouteSearchBar.IsFocused)
            {
                RouteSearchBar.Unfocus();
            }
            
            _logger?.LogDebug("Keyboard hidden by background tap");
        }
    }
	
	// Handle key press on custom keyboard
	private void OnKeyPressed(object? sender, EventArgs e)
	{
		if (sender is Button button)
		{
			// Append the key text to the search box
			_searchQuery += button.Text;
			RouteSearchBar.Text = _searchQuery;
			
			// Filter routes
			DebouncedFilterRoutes();
		}
	}
	
	// Handle alphabet key press
	private void OnAlphabetKeyPressed(object? sender, EventArgs e)
	{
		if (sender is Button button && button.CommandParameter is string key)
		{
			// Append the key text to the search box
			_searchQuery += key;
			RouteSearchBar.Text = _searchQuery;
			
			// Filter routes
			DebouncedFilterRoutes();
		}
	}
	
	// Handle delete key press
	private void OnDeletePressed(object? sender, EventArgs e)
	{
		if (_searchQuery.Length > 0)
		{
			// Remove the last character
			_searchQuery = _searchQuery.Substring(0, _searchQuery.Length - 1);
			RouteSearchBar.Text = _searchQuery;
			
			// Filter routes
			DebouncedFilterRoutes();
		}
	}
	
	// Handle clear key press
	private void OnClearPressed(object? sender, EventArgs e)
	{
		// Clear the search box
		_searchQuery = string.Empty;
		RouteSearchBar.Text = string.Empty;
		
		// Filter routes to show all
		DebouncedFilterRoutes();
	}
	
	// Handle search text changes
	private void OnSearchTextChanged(object? sender, TextChangedEventArgs e)
	{
		_searchQuery = e.NewTextValue ?? string.Empty;
		
		// Clear the cache when search query changes
		if (!string.IsNullOrEmpty(_searchQuery))
		{
			_stopGroupsCache.Clear();
		}
		
		OnPropertyChanged(nameof(HasSearchText));
		OnPropertyChanged(nameof(ClearButtonBackgroundColor));
		OnPropertyChanged(nameof(ClearButtonTextColor));
		OnPropertyChanged(nameof(DeleteButtonBackgroundColor));
		OnPropertyChanged(nameof(DeleteButtonTextColor));
		
		// If user types any text, automatically switch to ALL mode
		if (!string.IsNullOrEmpty(_searchQuery) && _currentDistanceFilter != DistanceFilter.All)
		{
			_currentDistanceFilter = DistanceFilter.All;
			UpdateDistanceFilterButtonColors();
		}
		
		// Apply filters
		if (_currentDistanceFilter != DistanceFilter.All && _userLocation != null)
		{
			// If we have a distance filter active, apply that with the search text
			// Show refresh indicator for visual feedback
			IsRefreshing = true;
			
			// Apply the filter in the background
			Task.Run(async () => 
			{
				try 
				{
					await ApplyDistanceFilterAsync(_currentDistanceFilter, _currentDistanceFilter);
				}
				finally 
				{
					await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
				}
			});
		}
		else
		{
			// Otherwise do normal text filtering
		DebouncedFilterRoutes();
		}
	}
	
	// Debounced version of filter routes to avoid excessive filtering
	private void DebouncedFilterRoutes()
	{
		try
		{
			lock (_filterLock)
			{
				// Cancel any previous filtering operation
				if (_filterCancellationTokenSource != null)
				{
					try 
					{
						if (!_filterCancellationTokenSource.IsCancellationRequested)
						{
							_filterCancellationTokenSource.Cancel();
						}
						_filterCancellationTokenSource.Dispose();
					}
					catch (ObjectDisposedException)
					{
						// Already disposed, ignore
					}
					catch (Exception ex)
					{
						_logger?.LogError(ex, "Error disposing CancellationTokenSource");
					}
				}
				
				_filterCancellationTokenSource = new CancellationTokenSource();
				
				var token = _filterCancellationTokenSource.Token;
				
				// Run debounced filter after a short delay
				Task.Delay(FilterDebounceMilliseconds, token).ContinueWith(async t => // Make ContinueWith lambda async
				{
					if (t.IsCanceled)
						return;
						
					// Run the actual filtering on a background thread
					await Task.Run(async () => { // Make Task.Run lambda async
						var filteredResultItems = FilterRoutesInternal(); // Returns List<object>
						await MainThread.InvokeOnMainThreadAsync(async () => { // Make lambda async
							await PopulateRoutesInBatchesAsync(filteredResultItems, _searchQuery);
							// KeyboardManager.UpdateAvailableRouteChars is now called inside PopulateRoutesInBatchesAsync
						});
					}, token);
				}, token);
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in debounced filter: {message}", ex.Message);
			
			// Ensure we still filter even if debouncing fails
			try
			{
				_ = Task.Run(async () => { // Make lambda async
					var resultItems = FilterRoutesInternal(); // Returns List<object>
					await MainThread.InvokeOnMainThreadAsync(async () => { // Make lambda async
						await PopulateRoutesInBatchesAsync(resultItems, _searchQuery);
						// KeyboardManager.UpdateAvailableRouteChars is now called inside PopulateRoutesInBatchesAsync
					});
				});
			}
			catch (Exception filterEx)
			{
				_logger?.LogError(filterEx, "Fallback filter also failed: {message}", filterEx.Message);
			}
		}
	}
	
	// Separate filtering logic from UI updates for better threading
	private List<object> FilterRoutesInternal() // Changed return type
	{
		// If a distance filter is active and not "ALL", use the distance filter instead
		if (_currentDistanceFilter != DistanceFilter.All && _userLocation != null)
		{
			// We should handle this differently - return empty list
			// as distance filter uses its own process
			return new List<object>();
		}
		
		// For empty search query with ALL filter, limit to an initial set
		// to avoid UI freezes, we'll load the rest incrementally later
		if (string.IsNullOrEmpty(_searchQuery) && _currentDistanceFilter == DistanceFilter.All)
		{
			// Return only 50 items initially for faster response (reduced from 100)
			return _allRoutes.Take(50).Cast<object>().ToList();
		}
		
		// For empty search query - handle the case separately for performance
		if (string.IsNullOrEmpty(_searchQuery))
		{
			// Check if we already have a cached list
			if (_allRoutesItemsCache != null && _allRoutesItemsCache.Count == _allRoutes.Count && _allRoutes.Count > 0)
			{
				// Use cached result
				return _allRoutesItemsCache;
			}
			
			// OPTIMIZATION: Create a list with preallocated capacity
			var result = new List<object>(_allRoutes.Count);
			
			// Use batched processing for better UI responsiveness
			const int batchSize = 500;
			
			for (int i = 0; i < _allRoutes.Count; i += batchSize)
			{
				var routesBatch = _allRoutes.Skip(i).Take(batchSize);
				foreach (var route in routesBatch)
				{
					result.Add(route);
				}
			}
			
			// Cache the result
			_allRoutesItemsCache = result;
			return result;
		}
		
		// Case with search query
		List<TransportRoute> filteredRoutesList;
		
		// Use trie for efficient search if available
		var searchResults = _routeTrie.Search(_searchQuery);
		if (searchResults.Any())
		{
			filteredRoutesList = searchResults;
		}
		else
		{
			// Fallback to manual search for partial matches
			var query = _searchQuery.ToUpperInvariant(); // Pre-compute this once
			
			// Create with appropriate capacity to avoid resizing
			filteredRoutesList = new List<TransportRoute>(Math.Min(100, _allRoutes.Count / 10));
			
			// Process in batches for better responsiveness
			const int batchSize = 500;
			
			for (int i = 0; i < _allRoutes.Count; i += batchSize)
			{
				var routesBatch = _allRoutes.Skip(i).Take(batchSize);
				foreach (var route in routesBatch)
				{
					if (route.RouteNumber.ToUpperInvariant().Contains(query))
					{
						filteredRoutesList.Add(route);
					}
				}
			}
		}
		
		// Convert to List<object> more efficiently
		var resultList = new List<object>(filteredRoutesList.Count);
		foreach (var route in filteredRoutesList)
		{
			resultList.Add(route);
		}
		return resultList;
	}
	
	// Filter routes based on search query - now just delegates to FilterRoutesInternal
	private void FilterRoutes()
	{
		// Run filtering on background thread
		Task.Run(async () => { // Make lambda async
			var filteredResultItems = FilterRoutesInternal(); // Now returns List<object>
			await MainThread.InvokeOnMainThreadAsync(async () => { // Make lambda async
				await PopulateRoutesInBatchesAsync(filteredResultItems, _searchQuery);
				// KeyboardManager.UpdateAvailableRouteChars is now called inside PopulateRoutesInBatchesAsync
			});
		});
	}
	
	// Optimized direct loading method
	private async Task LoadRoutesDirectly(bool showIndicators = true)
	{
		try 
		{
			if (showIndicators) IsRefreshing = true;
			
			// Add a semaphore to prevent concurrent database access
			using var semaphore = new SemaphoreSlim(1, 1); // Renamed from _dbAccessSemaphore to be local
			
			// Wait to acquire the semaphore with a timeout
			if (await semaphore.WaitAsync(5000)) // Increased timeout to 5 seconds
			{
				try
				{
					List<TransportRoute> allRoutesLocal = new List<TransportRoute>(); // Use a local variable
					
					await Task.Run(() => 
					{
						try 
						{
							var routes = _databaseService.GetAllRecords<TransportRoute>("TransportRoutes");
							if (routes != null)
							{
								allRoutesLocal = routes.ToList();
							}
						}
						catch (Exception ex)
						{
							_logger?.LogError(ex, "Error fetching routes from database in LoadRoutesDirectly");
						}
					});
					
					if (allRoutesLocal.Count > 0)
					{
						var sortedRoutes = new List<TransportRoute>();
						
						await Task.Run(() => 
						{
							sortedRoutes = SortRoutesNumerically(allRoutesLocal);
						});
						
						_allRoutes = sortedRoutes;

						// Populate _allRoutesDict
						try
                        {
                            _allRoutesDict = _allRoutes
                                .GroupBy(r => r.Id) // Ensure unique keys
                                .ToDictionary(g => g.Key, g => g.First());
                            if (_allRoutesDict.Count != _allRoutes.Count)
                            {
                                _logger?.LogWarning("LoadRoutesDirectly: Duplicate route IDs found. List count: {ListCount}, Dict count: {DictCount}. Some routes might be shadowed.", _allRoutes.Count, _allRoutesDict.Count);
                            }
                        }
                        catch (ArgumentNullException ex)
                        {
                            _logger?.LogError(ex, "LoadRoutesDirectly: Null key encountered while creating _allRoutesDict.");
                            _allRoutesDict = new Dictionary<string, TransportRoute>(); // Initialize to empty
                        }
                        catch (ArgumentException ex)
                        {
                            _logger?.LogError(ex, "LoadRoutesDirectly: Duplicate key encountered while creating _allRoutesDict (or other argument issue).");
                            _allRoutesDict = new Dictionary<string, TransportRoute>(); // Initialize to empty
                        }

						KeyboardManager.PrecomputeGloballyAvailableRouteChars(_allRoutes);
						
						await Task.Run(() => 
						{
							_routeTrie.Clear();
							const int trieChunkSize = 200;
							for (int i = 0; i < sortedRoutes.Count; i += trieChunkSize)
							{
								var chunk = sortedRoutes.Skip(i).Take(trieChunkSize);
								foreach (var route in chunk)
								{
									_routeTrie.AddRoute(route);
								}
							}
						});
						
						_logger?.LogInformation("_allRoutes populated with {ListCount} routes. _allRoutesDict created with {DictCount} routes.", _allRoutes.Count, _allRoutesDict.Count);
					}
					else
					{
						_logger?.LogWarning("LoadRoutesDirectly: No routes found in database or failed to load.");
                        _allRoutes = new List<TransportRoute>(); // Ensure initialized
                        _allRoutesDict = new Dictionary<string, TransportRoute>(); // Ensure initialized
					}
				}
				finally
				{
					semaphore.Release();
				}
			}
			else
			{
				_logger?.LogWarning("LoadRoutesDirectly: Could not acquire database semaphore within timeout - database may be busy");
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error loading routes directly in LoadRoutesDirectly");
		}
		finally
		{
			if (showIndicators)
			{
				await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
			}
		}
	}

	// Add the numerical route sorting method
	private List<TransportRoute> SortRoutesNumerically(List<TransportRoute> routes)
	{
		// Sort routes numerically by number part and then alphabetically by letter part
		return routes.OrderBy(r => {
			// Extract the numeric prefix from the route number
			string routeNum = r.RouteNumber ?? "";
			string numericPart = "";
			string alphaPart = "";
			
			// Parse the route number to separate numeric and alphabet parts
			int i = 0;
			while (i < routeNum.Length && char.IsDigit(routeNum[i]))
			{
				numericPart += routeNum[i];
				i++;
			}
			
			// Get the remaining part (letters)
			alphaPart = i < routeNum.Length ? routeNum.Substring(i) : "";
			
			// Parse the numeric part as an integer (defaulting to int.MaxValue if parsing fails)
			int numValue = int.TryParse(numericPart, out int num) ? num : int.MaxValue;
			
			return numValue; // First sort by the numeric value
		}).ThenBy(r => { 
			// Then sort by the letter part for routes with the same number
			string routeNum = r.RouteNumber ?? "";
			int i = 0;
			while (i < routeNum.Length && char.IsDigit(routeNum[i])) i++;
			return i < routeNum.Length ? routeNum.Substring(i) : "";
		}).ToList();
	}

	// Handle collection view scrolling
	private void OnCollectionViewScrolled(object? sender, ItemsViewScrolledEventArgs e)
	{
		// Store current scroll position unless we're in the middle of restoring it
		if (!_isRestoringScrollPosition && e.FirstVisibleItemIndex >= 0)
		{
			_lastScrolledItemIndex = e.FirstVisibleItemIndex;
			_lastScrolledItemOffset = e.CenterItemIndex;
		}

		// Hide keyboard when scrolling
		if (KeyboardManager.IsKeyboardVisible)
		{
			KeyboardManager.IsKeyboardVisible = false;
			
			if (RouteSearchBar != null)
			{
				RouteSearchBar.Unfocus();
			}
			
			_logger?.LogDebug("Keyboard hidden due to scrolling");
		}
	}
	
	// Size changed handler to update button widths
	protected override void OnSizeAllocated(double width, double height)
	{
		base.OnSizeAllocated(width, height);
		
		// If width changed significantly, update button widths
		if (Math.Abs(_lastWidth - width) > 1)
		{
			_lastWidth = width;
			KeyboardManager.UpdateKeyboardButtonWidths(width);
		}
	}

	/// <summary>
	/// Updates the user's location if it hasn't been updated recently
	/// </summary>
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
						
						// Check if we're running in a simulator (location will be null or have 0,0 coordinates)
						if (location == null || (location.Latitude == 0 && location.Longitude == 0))
						{
							_logger?.LogInformation("No valid location detected. Using default location for Hong Kong.");
							
							// Provide a default location in Hong Kong for simulator testing
							// Central MTR Station coordinates
							location = new Location(22.2830, 114.1583, 0);
						}
						
						if (location != null)
						{
							// Process location update on main thread
							await MainThread.InvokeOnMainThreadAsync(async () => {
								_userLocation = location;
								_lastLocationUpdate = DateTime.Now;
								_stopGroupsCache.Clear();
								
								// After getting new location, update nearby stops
								// This will also handle the current distance filter.
								if (_currentDistanceFilter != DistanceFilter.All)
								{
									_logger?.LogInformation("New location obtained. Nearby filter '{filter}' is active. Calling UpdateNearbyStops().", _currentDistanceFilter);
									IsRefreshing = true;
									try
									{
										await UpdateNearbyStops();
									}
									finally
									{
										IsRefreshing = false;
									}
								}
								else
								{
									 _logger?.LogInformation("New location obtained. Filter is ALL. OnAppearing or OnDistanceFilterClicked will handle list population.");
								}
							});
						}
						else
						{
							await MainThread.InvokeOnMainThreadAsync(async () => {
								_logger?.LogWarning("Could not get location, defaulting to ALL filter");
								_currentDistanceFilter = DistanceFilter.All;
								 UpdateDistanceFilterButtonColors();
								// FilterRoutes(); // Replaced by LoadAllRoutesWithVirtualization for consistency
								await LoadAllRoutesWithVirtualization();
							});
						}
					}
					catch (Exception ex)
					{
						_logger?.LogError(ex, "Error getting user location in background task");
						
						// On error, default to ALL filter
						await MainThread.InvokeOnMainThreadAsync(async () => {
							_currentDistanceFilter = DistanceFilter.All;
							 UpdateDistanceFilterButtonColors();
							// FilterRoutes(); // Replaced by LoadAllRoutesWithVirtualization for consistency
							await LoadAllRoutesWithVirtualization();
						});
					}
				});
			}
			else // Location is considered fresh
			{
				if (_currentDistanceFilter != DistanceFilter.All && _userLocation != null)
				{
					_logger?.LogInformation("Location is fresh. Current nearby filter is {filter}. Triggering UpdateNearbyStops.", _currentDistanceFilter);
					// Ensure IsRefreshing is managed on the UI thread
					await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = true);
					try
					{
						await UpdateNearbyStops(); // This will use the fresh _userLocation and current _currentDistanceFilter
					}
					finally
					{
						await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
					}
				}
				else if (_currentDistanceFilter == DistanceFilter.All)
				{
					_logger?.LogDebug("Location is fresh. Current filter is ALL. No direct stop update needed from this path as OnDistanceFilterClicked or OnAppearing handles it.");
				}
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
	
	/// <summary>
	/// Updates the colors of all distance filter buttons based on the current selection
	/// </summary>
	private void UpdateDistanceFilterButtonColors()
	{
		OnPropertyChanged(nameof(Distance100mButtonColor));
		OnPropertyChanged(nameof(Distance100mTextColor));
		OnPropertyChanged(nameof(Distance200mButtonColor));
		OnPropertyChanged(nameof(Distance200mTextColor));
		OnPropertyChanged(nameof(Distance400mButtonColor));
		OnPropertyChanged(nameof(Distance400mTextColor));
		OnPropertyChanged(nameof(Distance600mButtonColor));
		OnPropertyChanged(nameof(Distance600mTextColor));
		OnPropertyChanged(nameof(DistanceAllButtonColor));
		OnPropertyChanged(nameof(DistanceAllTextColor));
	}
	
	/// <summary>
	/// Updates the list of nearby stops for different distance ranges
	/// </summary>
	private async Task UpdateNearbyStops()
	{
		if (_userLocation == null)
		{
			_logger.LogWarning("UpdateNearbyStops: User location is null, cannot update nearby stops.");
			IsRefreshing = false;
			return;
		}

		try
		{
			// Save scroll position before updating nearby stops
			SaveScrollPosition();
			
			// Only show refresh indicator if the list is empty
			bool hasExistingItems = Routes != null && Routes.Count > 0;
			
			if (!hasExistingItems)
			{
				await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = true);
			}
			
			_logger.LogInformation("UpdateNearbyStops: Current user location is Lat={Lat}, Lng={Lng}", _userLocation.Latitude, _userLocation.Longitude);
			
			var userFullGeohash = NGeoHash.GeoHash.Encode(_userLocation.Latitude, _userLocation.Longitude, 8); 
			_logger.LogDebug("UpdateNearbyStops: User location geohash: CenterGH6={CenterGH6}, FullGH8={FullGH8}", 
				userFullGeohash.Substring(0, Math.Min(6, userFullGeohash.Length)), userFullGeohash);

			// Get nearby stops using the LocationCacheService instead of local cache or direct DB call
			List<TransportStop> nearbyStops = await _locationCacheService.GetNearbyStopsAsync(
				_userLocation.Latitude, _userLocation.Longitude, 1000);
				
			_logger.LogDebug("UpdateNearbyStops: Found {0} potential nearby stops", nearbyStops.Count);
			
			if (!nearbyStops.Any())
			{
				_logger.LogWarning("UpdateNearbyStops: No stops found in database for the current location.");
				// Clear refresh indicator
				await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
				return;
			}

			// Create concurrent bags for thread-safe collection
			var stops100m = new System.Collections.Concurrent.ConcurrentBag<TransportStop>();
			var stops200m = new System.Collections.Concurrent.ConcurrentBag<TransportStop>();
			var stops400m = new System.Collections.Concurrent.ConcurrentBag<TransportStop>();
			var stops600m = new System.Collections.Concurrent.ConcurrentBag<TransportStop>();

			// Process stops based on their distance (already calculated by LocationCacheService)
			foreach (var stop in nearbyStops)
			{
				// Add to appropriate distance buckets
				if (stop.DistanceFromUser <= 600)
				{
					stops600m.Add(stop);
					
					if (stop.DistanceFromUser <= 400)
					{
						stops400m.Add(stop);
						
						if (stop.DistanceFromUser <= 200)
						{
							stops200m.Add(stop);
							
							if (stop.DistanceFromUser <= 100)
							{
								stops100m.Add(stop);
							}
						}
					}
				}
			}

			// Store the results
			_stopsNearby["100m"] = stops100m.ToList();
			_stopsNearby["200m"] = stops200m.ToList();
			_stopsNearby["400m"] = stops400m.ToList();
			_stopsNearby["600m"] = stops600m.ToList();

			_logger.LogDebug("Found nearby stops: 100m={0}, 200m={1}, 400m={2}, 600m={3}",
				stops100m.Count, stops200m.Count, stops400m.Count, stops600m.Count);

			// IMPORTANT: Apply the current filter using the new incremental adaptation logic.
			if (_stopsNearby.Count > 0 && _currentDistanceFilter != DistanceFilter.All)
			{
				_logger.LogInformation("UpdateNearbyStops: Calling AdaptDisplayedStopsToFilterAsync for filter {filter}", _currentDistanceFilter);
				await AdaptDisplayedStopsToFilterAsync(_currentDistanceFilter);
			}
			else if (_currentDistanceFilter == DistanceFilter.All)
            {
                _logger.LogInformation("UpdateNearbyStops: Current filter is ALL. The UI will be populated by OnDistanceFilterClicked or OnAppearing via LoadAllRoutesWithVirtualization.");
                // If this method is called (e.g., from OnAppearing after location update) and the filter is ALL,
                // AdaptDisplayedStopsToFilterAsync itself handles calling LoadAllRoutesWithVirtualization.
                // So, we can also call it here to be safe, or rely on its internal handling for ALL.
                await AdaptDisplayedStopsToFilterAsync(DistanceFilter.All); 
            }
			else
			{
				_logger.LogInformation("UpdateNearbyStops: No nearby stops found or filter is ALL. Clearing/showing no routes.");
                // If no stops nearby but a distance filter is selected, clear the list.
                if (_currentDistanceFilter != DistanceFilter.All)
                {
                    await MainThread.InvokeOnMainThreadAsync(() => 
                    {
                        this.Routes.Clear();
                        var noRoutesLabel = this.FindByName<Microsoft.Maui.Controls.Label>("NoRoutesLabel");
                        if (noRoutesLabel != null)
                        {
                            noRoutesLabel.Text = $"No stops found within {_currentDistanceFilter.ToString().Replace("Meters", "")}m of your location.";
                            noRoutesLabel.IsVisible = true;
                        }
                    });
                }
			}
			
			// Always clear the refresh indicator when done
			await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error updating nearby stops");
			// Make sure to clear the refresh indicator in case of error
			await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
		}
	}

	/// <summary>
	/// Gets neighboring geohash cells to handle boundary cases
	/// </summary>
	private string[] GetNeighboringGeohashes(string centerGeohash)
	{
		// For simplicity, we'll just use a hardcoded list of directions to check
		// This avoids issues with the geohash library's specific implementation
		try 
		{
			// Get 8 neighbors
			var neighbors = NGeoHash.GeoHash.Neighbors(centerGeohash);
			// Add the center geohash itself
			var allRelevantGeohashes = new List<string>(neighbors);
			allRelevantGeohashes.Add(centerGeohash);
			
			_logger?.LogDebug("Using NGeoHash.Neighbors for geohashes: {0}", string.Join(", ", allRelevantGeohashes));
			return allRelevantGeohashes.ToArray();
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error calculating neighboring geohashes for {0}", centerGeohash);
			return new[] { centerGeohash }; // Return just the center if there's an error
		}
	}
	
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
				
				// Clear cache to ensure fresh data
				_stopGroupsCache.Clear();
				
				// Immediately update button colors to provide visual feedback
				UpdateDistanceFilterButtonColors();
				
				// Toggle visibility of views based on the selected filter
				bool isNearbyFilter = newFilter != DistanceFilter.All;
				IsNearbyViewVisible = isNearbyFilter;
				IsAllViewVisible = !isNearbyFilter;
				
				// Prepare loading UI in advance
				ActivityIndicator loadingIndicator = new ActivityIndicator
				{ 
					IsRunning = true,
					HorizontalOptions = LayoutOptions.Center,
					VerticalOptions = LayoutOptions.Center,
					Color = Color.FromArgb("#4B0082"), // Same as selected button color
					Scale = 1.5, // Make it larger and more visible
				};
				
				// Show small "loading" text below indicator
				Microsoft.Maui.Controls.Label loadingLabel = new Microsoft.Maui.Controls.Label
				{
					Text = "Loading routes...",
					HorizontalOptions = LayoutOptions.Center,
					VerticalOptions = LayoutOptions.Center,
					Margin = new Thickness(0, 5, 0, 0),
					TextColor = Color.FromArgb("#4B0082"),
					FontSize = 14
				};
				
				StackLayout loadingStack = new StackLayout
				{
					HorizontalOptions = LayoutOptions.Center,
					VerticalOptions = LayoutOptions.Center,
					Spacing = 5,
					Children = { loadingIndicator, loadingLabel }
				};
				
				// Get the appropriate collection view based on the selected filter
				CollectionView? collectionView = isNearbyFilter 
					? this.FindByName<CollectionView>("NearbyRoutesCollection")
					: this.FindByName<CollectionView>("AllRoutesCollection");
					
				if (collectionView != null && collectionView.Parent is Grid grid)
				{
					// Clear any existing routes to prevent UI freeze from rendering old content
					if (newFilter == DistanceFilter.All)
					{
						// Create empty collection to avoid freezing while clearing
						var emptyCollection = new ObservableRangeCollection<object>();
						Routes = emptyCollection;
					}
					else if (previousFilter == DistanceFilter.All)
					{
						// When switching from All to a distance filter, create an empty collection
						// to avoid rendering delays when the view is about to be hidden anyway
						var emptyCollection = new ObservableRangeCollection<object>();
						Routes = emptyCollection;
					}
					
					// Suspend any animation to reduce UI workload
					Microsoft.Maui.Controls.VisualStateManager.GoToState(collectionView, "NotAnimating");
					
					// Add the indicator at the center of the collection view
					grid.Add(loadingStack);
					
					Grid.SetRow(loadingStack, Grid.GetRow(collectionView));
					Grid.SetColumn(loadingStack, Grid.GetColumn(collectionView));
					
					// Show progress indicator but don't block UI with refresh indicator
					loadingStack.IsVisible = true;
				}
				
				// Very important: Let UI update before starting expensive operations
				// This prevents the UI freeze when the user taps the button
				Dispatcher.DispatchDelayed(TimeSpan.FromMilliseconds(50), () =>
				{
					// Apply the filter in the background to keep UI responsive
					_ = Task.Run(async () => 
					{
						try 
						{
							if (isNearbyFilter)
							{
								// This will ensure location is fresh if needed, then call UpdateNearbyStops with the new _currentDistanceFilter.
								// UpdateNearbyStops will then trigger the new data pipeline (ApplyDistanceFilterWithoutEtasAsync -> UpdateEtasForCurrentView).
								await UpdateUserLocationIfNeeded();
							}
							else
							{
								// For ALL filter - use revised approach that's optimized to prevent UI freezing
								
								// First, apply a small initial set (~20 routes) immediately for fast feedback
								var initialRoutes = _allRoutes.Take(20).Cast<object>().ToList();
								await MainThread.InvokeOnMainThreadAsync(() => 
								{
									var newCollection = new ObservableRangeCollection<object>();
									newCollection.BeginBatch();
									newCollection.AddRange(initialRoutes);
									newCollection.EndBatch();
									Routes = newCollection;
								});
								
								// Then load the rest in background with optimized batching
								await LoadAllRoutesWithVirtualization();
							}
						}
						catch (Exception ex)
						{
							_logger?.LogError(ex, "Error applying distance filter: {message}", ex.Message);
						}
						finally 
						{
							// Remove loading indicator when done
							await MainThread.InvokeOnMainThreadAsync(() => 
							{
								if (collectionView != null && collectionView.Parent is Grid parentGrid && loadingStack.Parent == parentGrid)
								{
									parentGrid.Remove(loadingStack);
								}
								
								// Resume animations
								if (collectionView != null)
								{
									Microsoft.Maui.Controls.VisualStateManager.GoToState(collectionView, "Normal");
								}
							});
						}
					});
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
			
			// Check if we have a cached result - use it immediately to prevent UI blocking
			if (_stopGroupsCache.ContainsKey(filter) && string.IsNullOrEmpty(_searchQuery))
			{
				return _stopGroupsCache[filter];
			}
			
			// Always use full processing for distance filter changes
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
			ObservableRangeCollection<object> filteredItems = new ObservableRangeCollection<object>();
			int totalRoutes = 0;
			string noRoutesText = string.Empty;
			
			// If no nearby stops found, show empty list
			if (nearbyStops.Count == 0)
			{
				noRoutesText = $"No stops found within {filter.ToString().Replace("Meters", "")} of your location.";
			}
			else
			{
				// Process the stop groups in background to avoid UI blocking
				await Task.Run(async () => {
					// Process all of the stop data off the UI thread to avoid blocking
					var stopGroups = new List<StopGroup>();
					
					// Get a list of stop IDs for batch processing
					var stopIds = nearbyStops.Select(s => s.Id).ToList();
					
					// Use the new batch method to get all routes for all stops at once
					var routesByStopId = await _databaseService.GetRoutesForStopsAsync(stopIds);
					
					// Now create the stop groups with distances - all calculated off the UI thread
					foreach (var stop in nearbyStops)
					{
						if (!routesByStopId.ContainsKey(stop.Id)) continue;

						// Apply text search filter if needed
						var sortedRoutes = routesByStopId[stop.Id];
						if (!string.IsNullOrEmpty(_searchQuery))
						{
							sortedRoutes = sortedRoutes.Where(r =>
								r.RouteNumber.Contains(_searchQuery, StringComparison.OrdinalIgnoreCase)).ToList();
						}
						if (!sortedRoutes.Any()) continue;
						totalRoutes += sortedRoutes.Count;

						// Calculate distance from user to this stop
						var stopLocation = new Location(stop.Latitude, stop.Longitude);
						double distanceInMeters = Location.CalculateDistance(_userLocation, stopLocation, DistanceUnits.Kilometers) * 1000;

						// Create and add a StopGroup object
						var stopGroup = new StopGroup(
							stop,
							sortedRoutes.OrderBy(r => r.RouteNumber).ToList(),
							distanceInMeters);

						stopGroups.Add(stopGroup);
					}

					// Sort all stop groups by distance
					stopGroups = stopGroups.OrderBy(sg => sg.DistanceInMeters).ToList();

					// Fetch ETAs in parallel for all stop groups (not just first batch)
					var etaTasks = stopGroups.Select(sg => FetchAndApplyEtasForStopGroup(sg));
					await Task.WhenAll(etaTasks);

					// Update UI with all stop groups
					await MainThread.InvokeOnMainThreadAsync(() => {
						filteredItems.BeginBatch();
						foreach (var sg in stopGroups)
						{
							if (sg != null && sg.Routes.Any())
							{
								filteredItems.Add(sg);
							}
						}
						filteredItems.EndBatch();
					});
				});
				
				if (filteredItems.Count == 0) // Check filteredItems (StopGroups) count
				{
					noRoutesText = $"No routes found within {filter.ToString().Replace("Meters", "")} of your location.";
				}
				
				_logger?.LogDebug("Found {0} routes from {1} stops near current location within {2}",
					totalRoutes, filteredItems.Count, filter.ToString().Replace("Meters", ""));
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
					
					// IMPORTANT: Force UI update for current view
					var collectionView = this.FindByName<CollectionView>("RoutesCollection");
					if (collectionView != null)
					{
						// Force collection view to update its ItemsSource
						collectionView.ItemsSource = null;
						collectionView.ItemsSource = filteredItems;
					}
					
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
	private void OnStopGroupTapped(object sender, TappedEventArgs e)
	{
		try
		{
			// Get the StopGroup from the CommandParameter
			if (e.Parameter is StopGroup stopGroup)
			{
				// Toggle expansion state only
				stopGroup.IsExpanded = !stopGroup.IsExpanded;
				
				_logger?.LogDebug("Toggled expansion for stop {stopName} (ID: {stopId}) to {isExpanded}", 
					stopGroup.Stop.LocalizedName, stopGroup.Stop.StopId, stopGroup.IsExpanded);
			}
			else if (sender is BindableObject bindable && bindable.BindingContext is StopGroup sg)
			{
				// Fallback to get from binding context if needed
				sg.IsExpanded = !sg.IsExpanded;
				
				_logger?.LogDebug("Toggled expansion for stop {stopName} (ID: {stopId}) to {isExpanded} (via BindingContext)", 
					sg.Stop.LocalizedName, sg.Stop.StopId, sg.IsExpanded);
			}
			
			// Ensure the timer is running for periodic updates
			EnsureEtaTimerIsRunning();
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in OnStopGroupTapped");
		}
	}
	
	// New method to fetch ETAs for a specific stop group
	private async void FetchEtaForStopGroup(StopGroup stopGroup)
	{
		try
		{
			// Skip if update is already in progress
			if (_isUpdatingEtas)
			{
				_logger?.LogDebug("Skipping specific ETA update as one is already in progress");
				return;
			}

			_logger?.LogDebug("Fetching ETAs for stop: {stopId} ({stopName})", 
				stopGroup.Stop.StopId, stopGroup.Stop.LocalizedName);
				
			// Check if it's an MTR stop or KMB stop
			List<TransportEta> allEtas;
			if (stopGroup.Stop.Id.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase))
			{
				_logger?.LogDebug("Fetching MTR ETAs for stop: {stopId}", stopGroup.Stop.Id);
				allEtas = await _etaService.FetchAllMtrEtaForStop(stopGroup.Stop.Id);
			}
			else
			{
				_logger?.LogDebug("Fetching KMB ETAs for stop: {stopId}", stopGroup.Stop.Id);
				allEtas = await _etaService.FetchAllKmbEtaForStop(stopGroup.Stop.Id);
			}
			
			if (allEtas.Count == 0)
			{
				_logger?.LogDebug("No ETAs found for stop: {stopId}", stopGroup.Stop.Id);
				return;
			}
			
			_logger?.LogDebug("Found {count} ETAs for stop: {stopId}", allEtas.Count, stopGroup.Stop.Id);
			
			// Update the UI on the main thread
			await MainThread.InvokeOnMainThreadAsync(() =>
			{
				try
				{
					// Group ETAs by route number and service type
					var etasByRoute = allEtas.GroupBy(e => new { RouteNumber = e.RouteNumber, ServiceType = e.ServiceType })
						.ToDictionary(g => g.Key, g => g.ToList());
					
					int routesUpdated = 0;
					
					// Update each route with its ETAs
					foreach (var route in stopGroup.Routes)
					{
						var key = new { RouteNumber = route.RouteNumber, ServiceType = route.ServiceType };
						if (etasByRoute.TryGetValue(key, out var etas) && etas.Count > 0)
						{
							// Update the route's FirstEta property with the first (earliest) ETA's display text
							route.FirstEta = etas.OrderBy(e => e.EtaTime).FirstOrDefault()?.DisplayEta ?? string.Empty;
							
							// Also update the StopGroup's ETAs collection
							stopGroup.UpdateEtas(route.RouteNumber, route.ServiceType, route.Bound, etas);
							
							routesUpdated++;
						}
					}
					
					_logger?.LogDebug("Updated ETAs for {updated}/{total} routes at stop {stopId}", 
						routesUpdated, stopGroup.Routes.Count, stopGroup.Stop.Id);
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error updating ETAs on main thread");
				}
			});
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error fetching ETAs for stop {stopName}", stopGroup.Stop.LocalizedName);
		}
	}
	
	// Ensure the global ETA update timer is running
	private void EnsureEtaTimerIsRunning()
	{
		try 
		{
			lock (_timerLock)
			{
				if (_etaTimerRunning) 
					return;
				
				_etaTimerRunning = true;
				
				// Create a timer if it doesn't exist
				if (_etaUpdateTimer == null)
				{
					_etaUpdateTimer = Dispatcher.CreateTimer();
					// Increase interval to reduce API calls and prevent UI freezing
					_etaUpdateTimer.Interval = TimeSpan.FromSeconds(45); // Increased from 30 to 45 seconds
					_etaUpdateTimer.Tick += EtaUpdateTimer_Tick;
					
					_logger?.LogInformation("Created ETA update timer with interval {0} seconds", _etaUpdateTimer.Interval.TotalSeconds);
				}
				
				// Start the timer if it exists and is not running
				if (_etaUpdateTimer != null && !_etaUpdateTimer.IsRunning)
				{
					// Important: Delay the first execution to avoid UI freezes during initial load
					Task.Delay(3000).ContinueWith(_ => {
						MainThread.BeginInvokeOnMainThread(() => {
							if (_etaUpdateTimer != null && !_etaUpdateTimer.IsRunning)
							{
								_etaUpdateTimer.Start();
								_logger?.LogInformation("Started global ETA update timer");
							}
						});
					});
				}
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error starting ETA timer");
		}
	}
	
	// Timer tick handler for ETA updates
	private void EtaUpdateTimer_Tick(object? sender, EventArgs e)
	{
		try
		{
			if (!_etaTimerRunning)
			{
				if (_etaUpdateTimer?.IsRunning == true)
				{
					_etaUpdateTimer.Stop();
					_logger?.LogDebug("Stopped ETA timer as _etaTimerRunning is false");
				}
				return;
			}
			
			_logger?.LogDebug("ETA update timer tick - scheduling update for expanded stop groups");
			
			// Important: Run on a background thread to avoid blocking UI
			Task.Run(async () => {
				try
				{
					// Add some randomization to avoid all clients hitting the API at the same time
					int randomDelay = new Random().Next(100, 1000);
					await Task.Delay(randomDelay);
					
					// IMPORTANT: Always use the version without refresh indicator for timer-based background updates
					// This is a background update that should never affect the UI's scroll position
					await UpdateExpandedStopGroupEtas();
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error in background ETA update");
				}
			});
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in ETA timer tick handler");
		}
	}
	
	// New method to update ETAs for all expanded stop groups
	private async Task UpdateExpandedStopGroupEtas()
	{
		try
		{
			// Check if an update is already in progress
			if (_isUpdatingEtas)
			{
				_logger?.LogDebug("Skipping scheduled ETA update as one is already in progress");
				return;
			}
			
			// Save scroll position before updating ETAs
			SaveScrollPosition();
			
			// Use a lock to ensure only one update runs at a time
			lock (_etaLock)
			{
				if (_isUpdatingEtas) return;
				_isUpdatingEtas = true;
			}
			
			// IMPORTANT: NEVER show refresh indicator for background updates
			// We'll never set IsRefreshing = true in this method
			
			try
			{
				if (Routes == null || Routes.Count == 0)
				{
					_logger?.LogDebug("No routes to update ETAs for");
					return;
				}
				
				// Get all expanded stop groups
				var expandedStopGroups = Routes.OfType<StopGroup>()
					.Where(sg => sg.IsExpanded)
					.ToList();
				
				if (expandedStopGroups.Count == 0)
				{
					_logger?.LogDebug("No expanded stop groups to update");
					return;
				}
				
				_logger?.LogInformation("Updating ETAs for {count} expanded stop groups", expandedStopGroups.Count);
				
				// Use a semaphore to limit concurrent API requests
				using var semaphore = new SemaphoreSlim(2); // Reduce from 3 to 2 max concurrent requests
				
				// Group stops by stop ID to avoid redundant calls
				var stopsByStopId = expandedStopGroups.GroupBy(sg => sg.Stop.StopId)
					.ToDictionary(g => g.Key, g => g.ToList());
					
				_logger?.LogDebug("Grouped {originalCount} stop groups into {groupedCount} unique stop IDs", 
					expandedStopGroups.Count, stopsByStopId.Count);
					
				// Process in small batches to reduce UI impact
				var stopIds = stopsByStopId.Keys.ToList();
				const int batchSize = 3; // Process at most 3 stops at a time
				
				for (int i = 0; i < stopIds.Count; i += batchSize)
				{
					// Get current batch
					var currentBatch = stopIds.Skip(i).Take(batchSize).ToList();
					
					// Process each stop in current batch with limited concurrency
					var batchTasks = currentBatch.Select(async stopId => {
						try
						{
							await semaphore.WaitAsync();
							try
							{
								// Check if we already have fresh ETAs for this stop in cache
								if (_etaService.HasCachedEtas(stopId))
								{
									_logger?.LogDebug("Using cached ETAs for stop: {stopId}", stopId);
									var cachedEtas = await _etaService.GetCachedEtas(stopId);
									
									// Apply the cached ETAs to all stop groups with this ID
									if (stopsByStopId.TryGetValue(stopId, out var cachedStopGroups))
									{
										foreach (var stopGroup in cachedStopGroups)
										{
											await ApplyEtasToStopGroupAsync(stopGroup, cachedEtas);
										}
									}
									
									return;
								}
								
								// Get all ETAs for this stop
								_logger?.LogDebug("Fetching ETAs for stop: {stopId}", stopId);
								
								List<TransportEta> allEtas;
								if (stopId.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase))
								{
									_logger?.LogDebug("Fetching MTR ETAs for stop: {stopId}", stopId);
									allEtas = await _etaService.FetchAllMtrEtaForStop(stopId);
								}
								else
								{
									_logger?.LogDebug("Fetching KMB ETAs for stop: {stopId}", stopId);
									allEtas = await _etaService.FetchAllKmbEtaForStop(stopId);
								}
								
								if (allEtas.Count == 0)
								{
									_logger?.LogDebug("No ETAs found for stop: {stopId}", stopId);
									return;
								}
								
								_logger?.LogDebug("Found {count} ETAs for stop: {stopId}", allEtas.Count, stopId);
								
								// Apply the ETAs to all stop groups with this ID
								if (stopsByStopId.TryGetValue(stopId, out var relatedStopGroups))
								{
									foreach (var stopGroup in relatedStopGroups)
									{
										await ApplyEtasToStopGroupVoidAsync(stopGroup, allEtas);
									}
								}
							}
							finally
							{
								semaphore.Release();
							}
						}
						catch (Exception ex)
						{
							_logger?.LogError(ex, "Error fetching ETAs for stop {stopId}", stopId);
						}
					});
					
					// Wait for current batch to complete
					await Task.WhenAll(batchTasks);
					
					// Add a small delay between batches to keep UI responsive
					if (i + batchSize < stopIds.Count)
					{
						await Task.Delay(100);
					}
				}
				
				_logger?.LogInformation("Completed ETA updates for all expanded stop groups");
				
				// IMPORTANT: Don't call OnPropertyChanged(nameof(Routes)) here as it resets scroll position
				// Instead, individual item property changes will propagate through data binding
				
				// Call cleanup after processing all ETAs for expanded groups
				await CleanupEmptyStopGroupsFromViewAsync();
				
				// Restore scroll position after updates
				await RestoreScrollPosition();
			}
			finally
			{
				// Reset the update flag
				lock (_etaLock)
				{
					_isUpdatingEtas = false;
				}
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error updating ETAs for expanded stop groups");
			
			// Reset the update flag on error
			lock (_etaLock)
			{
				_isUpdatingEtas = false;
			}
		}
	}
	
	// Direct event handler for tapping a nested route
	private async void OnNestedRouteTapped(object sender, TappedEventArgs e)
	{
		try
		{
			TransportRoute? selectedRoute = null;
			
			if (sender is Grid grid && grid.BindingContext is TransportRoute route)
			{
				selectedRoute = route;
			}
			
			if (selectedRoute != null)
			{
				// Hide keyboard if visible
				if (KeyboardManager.IsKeyboardVisible)
				{
					KeyboardManager.IsKeyboardVisible = false;
				}
				
				// Show loading indicator
				IsBusy = true;
				
				// Start loading the stops data in the background but don't await it
				// This allows the navigation to happen immediately
				_ = Task.Run(() => _databaseService.GetSortedStopsForRoute(selectedRoute.Id));
				
				// Create the page instance ahead of time
				RouteDetailsPage? detailsPage = null;
				
				try
				{
					detailsPage = Handler?.MauiContext?.Services?.GetService<RouteDetailsPage>();
					
					if (detailsPage == null)
					{
						// Log fallback
						_logger?.LogWarning("RouteDetailsPage could not be resolved from DI. Creating manually.");
						detailsPage = new RouteDetailsPage(_databaseService, _etaService, new LoggerFactory().CreateLogger<RouteDetailsPage>());
					}
					
					// Set the route on the details page
					detailsPage.SetRoute(selectedRoute);
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error creating RouteDetailsPage");
					await DisplayAlert("Error", "Could not create route details page.", "OK");
					IsBusy = false;
					return;
				}
				
				// Clear IsBusy before navigation to avoid visual glitches
				IsBusy = false;
				
				// Navigate to the route details page
				await Navigation.PushAsync(detailsPage);
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in OnNestedRouteTapped");
			await DisplayAlert("Error", "Could not navigate to route details.", "OK");
			IsBusy = false;
		}
	}

	// OnRouteTapped handler for the "ALL" view
	private async void OnRouteTapped(object? sender, TappedEventArgs e)
	{
		try 
		{
			// Only handle taps when the item is a TransportRoute directly in the list (ALL mode)
			TransportRoute? selectedRoute = null;
			
			if (sender is Border border && border.BindingContext is TransportRoute route)
			{
				selectedRoute = route;
			}
			else if (sender is Grid grid && grid.BindingContext is TransportRoute gridRoute)
			{
				// Handle taps on Grid items (like the favorite route)
				selectedRoute = gridRoute;
			}
			
			if (selectedRoute != null)
			{
				// Hide keyboard if visible
				if (KeyboardManager.IsKeyboardVisible)
				{
					KeyboardManager.IsKeyboardVisible = false;
				}
				
				// Show loading indicator
				IsBusy = true;
				
				// Start loading the stops data in the background but don't await it
				// This allows the navigation to happen immediately
				_ = Task.Run(() => _databaseService.GetSortedStopsForRoute(selectedRoute.Id));
				
				// Create the page instance ahead of time
				RouteDetailsPage? detailsPage = null;
				
				try
				{
					detailsPage = Handler?.MauiContext?.Services?.GetService<RouteDetailsPage>();
					
					if (detailsPage == null)
					{
						// Log fallback
						_logger?.LogWarning("RouteDetailsPage could not be resolved from DI. Creating manually.");
						detailsPage = new RouteDetailsPage(_databaseService, _etaService, new LoggerFactory().CreateLogger<RouteDetailsPage>());
					}
					
					// Set the route on the details page
					detailsPage.SetRoute(selectedRoute);
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error creating RouteDetailsPage");
					await DisplayAlert("Error", "Could not create route details page.", "OK");
					IsBusy = false;
					return;
				}
				
				// Clear IsBusy before navigation to avoid visual glitches
				IsBusy = false;
				
				// Navigate to the route details page
				await Navigation.PushAsync(detailsPage);
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in OnRouteTapped: {message}", ex.Message);
			
			// If an error occurs, show an alert to the user
			await DisplayAlert("Error", "Could not navigate to route details.", "OK");
			
			// Reset IsBusy to false to hide the loading indicator
			IsBusy = false;
		}
	}

	// Handler for the Favorites section header
	private bool _isFavoritesExpanded = true;
	private void OnFavoriteHeaderTapped(object sender, TappedEventArgs e)
	{
		try
		{
			// Toggle the expanded state
			_isFavoritesExpanded = !_isFavoritesExpanded;
			
			// Find the favorite routes grid by name
			Grid favoriteGrid = this.FindByName<Grid>("FavoriteRoutesGrid");
			if (favoriteGrid != null && favoriteGrid.RowDefinitions.Count >= 2)
			{
				// If we found the grid, toggle visibility of the second row
				// which contains the favorite routes list
				if (favoriteGrid.Children.Count > 1)
				{
					var routesContent = favoriteGrid.Children[1] as Microsoft.Maui.Controls.VisualElement;
					if (routesContent != null)
					{
						routesContent.IsVisible = _isFavoritesExpanded;
					}
				}
				
				_logger?.LogDebug("Toggled favorites expansion to {isExpanded}", _isFavoritesExpanded);
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error toggling favorites section");
		}
	}

	// Add a method to fetch and update ETAs for the routes in stopGroups
	private async Task FetchAndUpdateEtas()
	{
		// Check if an update is already in progress
		if (_isUpdatingEtas)
		{
			_logger?.LogDebug("Skipping ETA update as one is already in progress");
			return;
		}
		
		// Use a lock to ensure only one update runs at a time
		lock (_etaLock)
		{
			if (_isUpdatingEtas) return;
			_isUpdatingEtas = true;
		}
		
		try
		{
			if (Routes == null || Routes.Count == 0)
			{
				_logger?.LogDebug("No routes to update ETAs for");
				return;
			}

			var stopGroups = Routes.OfType<StopGroup>().ToList();
			if (stopGroups.Count == 0)
			{
				_logger?.LogDebug("No StopGroups found in Routes collection");
				return;
			}

			_logger?.LogInformation("Starting ETA updates for {count} stop groups", stopGroups.Count);

			// First prioritize visible/expanded stop groups
			var prioritizedStopGroups = stopGroups
				.OrderByDescending(sg => sg.IsExpanded)
				.ToList();

			// Group stops by stop ID to avoid redundant calls
			var stopsByStopId = prioritizedStopGroups.GroupBy(sg => sg.Stop.StopId)
				.ToDictionary(g => g.Key, g => g.ToList());
				
			_logger?.LogDebug("Grouped {originalCount} stop groups into {groupedCount} unique stop IDs", 
				stopGroups.Count, stopsByStopId.Count);

			// Process stops in batches to avoid overwhelming the API
			const int batchSize = 5;
			var stopIds = stopsByStopId.Keys.ToList();
			
			// Use a SemaphoreSlim to limit concurrent API requests
			using var semaphore = new SemaphoreSlim(3); // Max 3 concurrent requests
			
			for (int i = 0; i < stopIds.Count; i += batchSize)
			{
				// Get the current batch of stop IDs
				var currentBatch = stopIds.Skip(i).Take(batchSize).ToList();
				
				_logger?.LogDebug("Processing batch {current}/{total} of stop IDs for ETAs", 
					i/batchSize + 1, (int)Math.Ceiling(stopIds.Count / (double)batchSize));
				
				// Process each stop ID in the batch with limited concurrency
				var tasks = currentBatch.Select(async stopId =>
				{
					try
					{
						await semaphore.WaitAsync();
						try
						{
							// Check if we already have fresh ETAs for this stop in cache
							if (_etaService.HasCachedEtas(stopId))
							{
								_logger?.LogDebug("Using cached ETAs for stop: {stopId}", stopId);
								var cachedEtas = await _etaService.GetCachedEtas(stopId);
								
								// Apply the cached ETAs to all stop groups with this ID
								if (stopsByStopId.TryGetValue(stopId, out var cachedStopGroups))
								{
									foreach (var stopGroup in cachedStopGroups)
									{
										await ApplyEtasToStopGroupAsync(stopGroup, cachedEtas);
									}
								}
								
								return;
							}
							
							// Get all ETAs for this stop (more efficient than fetching per route)
							_logger?.LogDebug("Fetching ETAs for stop: {stopId}", stopId);
							var allEtas = await _etaService.FetchAllKmbEtaForStop(stopId);
							
							// Apply the ETAs to all stop groups with this ID
							if (stopsByStopId.TryGetValue(stopId, out var relatedStopGroups))
							{
								foreach (var stopGroup in relatedStopGroups)
								{
									await ApplyEtasToStopGroupAsync(stopGroup, allEtas);
								}
							}
						}
						finally
						{
							semaphore.Release();
						}
					}
					catch (Exception ex)
					{
						_logger?.LogError(ex, "Error fetching ETAs for stop {stopId}", stopId);
					}
				});
				
				// Wait for all tasks in this batch to complete
				await Task.WhenAll(tasks);
				
				// Add a small delay between batches to avoid rate limiting
				if (i + batchSize < stopIds.Count)
				{
					await Task.Delay(200);
				}
			}
			
			_logger?.LogInformation("Completed ETA updates for all stop groups");
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error fetching ETAs for nearby stops");
		}
		finally
		{
			// Reset the update flag
			lock (_etaLock)
			{
				_isUpdatingEtas = false;
			}
		}
	}

	// Helper method to apply ETAs to a stop group
	private async Task<bool> ApplyEtasToStopGroupAsync(StopGroup stopGroup, List<TransportEta> allEtasFromApi)
	{
		try
		{
			bool anyRouteHasSuccessfullyReceivedEta = false; 

			if (stopGroup?.Stop == null) // Combined null check for stopGroup and stopGroup.Stop
			{
				_logger?.LogError("ApplyEtasToStopGroupAsync: stopGroup or stopGroup.Stop is null. Cannot apply ETAs. StopGroup: {SG}, Stop: {S}", 
					stopGroup == null ? "null" : "valid", 
					stopGroup?.Stop == null ? "null" : "valid");
				return false;
			}
			
			string stopIdForLog = stopGroup.Stop.StopId ?? "UNKNOWN_STOP_ID";
			string stopNameForLog = stopGroup.Stop.LocalizedName ?? "Unknown Stop";

			if (allEtasFromApi == null) 
			{
				_logger?.LogWarning("ApplyEtasToStopGroupAsync: allEtasFromApi is null for StopGroup {StopId} ({StopName}). Clearing ETAs.", stopIdForLog, stopNameForLog);
				await MainThread.InvokeOnMainThreadAsync(() =>
				{
					if (stopGroup.Routes != null)
					{
						foreach (var route in stopGroup.Routes)
						{
							if (route == null) continue;
							route.FirstEta = string.Empty;
							route.NextEta = null;
							route.Etas = new List<TransportEta>();
						}
					}
					stopGroup.IsExpanded = false; // Collapse if no data
				});
				// anyRouteHasSuccessfullyReceivedEta remains false
			}
			else if (!allEtasFromApi.Any()) // API returned an empty list
			{
				_logger?.LogDebug("ApplyEtasToStopGroupAsync: API returned 0 ETAs for stop {StopId} ({StopName}). Marking all routes as having no ETA.", stopIdForLog, stopNameForLog);
				await MainThread.InvokeOnMainThreadAsync(() =>
				{
					if (stopGroup.Routes != null)
					{
						foreach (var route in stopGroup.Routes)
						{
							if (route == null) continue;
							route.FirstEta = string.Empty;
							route.NextEta = null;
							route.Etas = new List<TransportEta>();
						}
					}
					stopGroup.IsExpanded = false; // Collapse if no data
				});
				// anyRouteHasSuccessfullyReceivedEta remains false
			}
			else // ETAs are present and the list is not empty
			{
				var updates = new List<Tuple<TransportRoute, TransportEta?, List<TransportEta>>>();
				
				// Create different dictionaries for matching ETAs based on operator
				Dictionary<object, List<TransportEta>> etasByRouteKey;
				
				if (stopGroup.Stop.Operator == TransportOperator.MTR)
				{
					// For MTR, use a simpler matching approach with just the route number
					etasByRouteKey = allEtasFromApi
						.GroupBy(e => e.RouteNumber)
						.ToDictionary(g => (object)g.Key, g => g.ToList());
					_logger?.LogDebug("Created MTR ETA dictionary with {count} route keys for stop {stopId}", 
						etasByRouteKey.Count, stopIdForLog);
				}
				else
				{
					// For other operators, use the more specific key
					etasByRouteKey = allEtasFromApi
						.GroupBy(e => new { e.RouteNumber, e.ServiceType, e.Direction }) 
						.ToDictionary(g => (object)g.Key, g => g.ToList());
				}

				// Track which route keys we process to identify new routes with ETAs later
				HashSet<object> processedRouteKeys = new HashSet<object>();
				
				if (stopGroup.Routes != null)
				{
					foreach (var route in stopGroup.Routes) 
					{
						if (route == null) continue;
						
						bool foundEta = false;
						List<TransportEta>? etasForRoute = null;
						
						if (route.Operator == TransportOperator.MTR)
						{
							// For MTR routes, match just by route number
							if (etasByRouteKey.TryGetValue(route.RouteNumber, out etasForRoute) && etasForRoute.Any())
							{
								var firstEta = etasForRoute.OrderBy(e => e.EtaTime).FirstOrDefault();
								updates.Add(Tuple.Create(route, firstEta, etasForRoute));
								processedRouteKeys.Add(route.RouteNumber);
								foundEta = true;
								_logger?.LogDebug("Found ETAs for MTR route {routeNumber} at stop {stopName}", 
									route.RouteNumber, stopGroup.Stop.LocalizedName);
							}
						}
						else
						{
							// For other operators, use the more specific key
							var key = new { route.RouteNumber, route.ServiceType, Direction = route.Bound };
							if (etasByRouteKey.TryGetValue(key, out etasForRoute) && etasForRoute.Any())
							{
								var firstEta = etasForRoute.OrderBy(e => e.EtaTime).FirstOrDefault();
								updates.Add(Tuple.Create(route, firstEta, etasForRoute));
								processedRouteKeys.Add(key);
								foundEta = true;
							}
						}
						
						if (!foundEta)
						{
							updates.Add(Tuple.Create(route, (TransportEta?)null, new List<TransportEta>()));
						}
					}
				}
				
				// For MTR stops, check if there are any routes with ETAs that aren't in the stop group yet
				List<TransportRoute> newRoutesToAdd = new List<TransportRoute>();
				
				if (stopGroup.Stop.Id.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase))
				{
					foreach (var kvp in etasByRouteKey)
					{
						// Skip routes we've already processed
						if (processedRouteKeys.Contains(kvp.Key))
							continue;
							
						var routeKey = kvp.Key;
						var etasForRoute = kvp.Value;
						
						if (etasForRoute.Any() && !string.IsNullOrEmpty(etasForRoute[0].RouteId))
						{
							// Get this route from the database
							var route = await _databaseService.GetRouteByIdAsync(etasForRoute[0].RouteId);
							if (route != null)
							{
								// Add it to our list of routes to add to the stop group
								var firstEta = etasForRoute.OrderBy(e => e.EtaTime).FirstOrDefault();
								route.NextEta = firstEta;
								route.Etas = etasForRoute;
								
								// Calculate and set the FirstEta display value
								if (firstEta != null)
								{
									TimeSpan timeDiff = firstEta.EtaTime - DateTime.Now;
									if (timeDiff.TotalMinutes <= 0)
										route.FirstEta = "Arrived";
									else if (timeDiff.TotalMinutes < 1)
										route.FirstEta = "Arriving";
									else
										route.FirstEta = $"{(int)Math.Floor(timeDiff.TotalMinutes)} min";
								}
								
								newRoutesToAdd.Add(route);
								_logger?.LogDebug("Found MTR route {routeNumber} for stop {stopName} based on valid ETAs", 
									route.RouteNumber, stopGroup.Stop.LocalizedName);
							}
						}
					}
				}

				await MainThread.InvokeOnMainThreadAsync(() =>
				{
					try
					{
						bool atLeastOneRouteInGroupGotEtaThisTime = false;
						if (stopGroup.Routes != null) 
						{
							foreach (var update in updates)
							{
								var routeToUpdate = update.Item1;
								if (routeToUpdate == null) continue;

								routeToUpdate.FirstEta = update.Item2?.DisplayEta ?? string.Empty;
								routeToUpdate.NextEta = update.Item2;
								routeToUpdate.Etas = update.Item3 ?? new List<TransportEta>();
								
								if (routeToUpdate.HasEta) 
								{
									atLeastOneRouteInGroupGotEtaThisTime = true;
								}
							}
							
							// Add any new MTR routes with valid ETAs
							foreach (var newRoute in newRoutesToAdd)
							{
								stopGroup.Routes.Add(newRoute);
								atLeastOneRouteInGroupGotEtaThisTime = true;
							}
							
							if (newRoutesToAdd.Count > 0)
							{
								_logger?.LogInformation("Added {count} new MTR routes to stop {stopName} based on valid ETAs",
									newRoutesToAdd.Count, stopGroup.Stop.LocalizedName);
							}
						}
						// The StopGroup's HasEtas property should update based on its internal logic
						// when its routes' ETAs change. We don't set it directly here.
						// If atLeastOneRouteInGroupGotEtaThisTime is false, we might want to collapse the group.
						if (!atLeastOneRouteInGroupGotEtaThisTime)
						{
							stopGroup.IsExpanded = false;
						}
						anyRouteHasSuccessfullyReceivedEta = atLeastOneRouteInGroupGotEtaThisTime;
					}
					catch (Exception ex)
					{
						_logger?.LogError(ex, "Error during MainThread update of route properties in ApplyEtasToStopGroupAsync for StopGroup {StopId} ({StopName})", stopIdForLog, stopNameForLog);
						anyRouteHasSuccessfullyReceivedEta = false; 
						stopGroup.IsExpanded = false; // Collapse on error too
					}
				});
			}
			
			_logger?.LogDebug("ApplyEtasToStopGroupAsync for StopGroup '{StopName}' (ID: {StopId}): Processed. anyRouteGotEta: {AnyEta}", 
				stopNameForLog, stopIdForLog, anyRouteHasSuccessfullyReceivedEta);
			return anyRouteHasSuccessfullyReceivedEta;
		}
		catch (Exception ex)
		{
			string stopIdForCatch = stopGroup?.Stop?.StopId ?? "unknown_in_catch";
			string stopNameForCatch = stopGroup?.Stop?.LocalizedName ?? "unknown_in_catch";
			_logger?.LogError(ex, "Outer error in ApplyEtasToStopGroupAsync for stop {StopName} (ID: {StopId})", stopNameForCatch, stopIdForCatch);
			
			if (stopGroup?.Stop != null) 
			{
				await MainThread.InvokeOnMainThreadAsync(() => {
					try 
					{
						if (stopGroup.Routes != null) 
						{
							foreach (var route in stopGroup.Routes) 
							{
								if (route == null) continue;
								route.FirstEta = string.Empty;
								route.NextEta = null;
								route.Etas = new List<TransportEta>();
							}
						}
						stopGroup.IsExpanded = false; // Collapse on outer error
					} 
					catch (Exception iex) 
					{
						_logger?.LogError(iex, "Failed to clear ETAs on routes in outer catch block of ApplyEtasToStopGroupAsync for stop {StopId}.", stopIdForCatch);
					}
				});
			}
			return false; 
		}
	}

	// New method to fetch and apply ETAs for a specific stop group
	private async Task<bool> FetchAndApplyEtasForStopGroup(StopGroup stopGroup)
	{
		try
		{
			_logger?.LogDebug("Fetching ETAs for stop: {stopId} ({stopName})", 
				stopGroup.Stop.StopId, stopGroup.Stop.LocalizedName);
				
			// Check if we already have fresh ETAs for this stop in cache
			if (_etaService.HasCachedEtas(stopGroup.Stop.StopId))
			{
				_logger?.LogDebug("Using cached ETAs for stop: {stopId}", stopGroup.Stop.StopId);
				var cachedEtas = await _etaService.GetCachedEtas(stopGroup.Stop.StopId);
				return await ApplyEtasToStopGroupAsync(stopGroup, cachedEtas);
			}
				
			// Get ETAs for this stop - check if it's an MTR stop or KMB stop
			List<TransportEta> allEtas;
			
			if (stopGroup.Stop.Id.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase))
			{
				_logger?.LogDebug("Fetching MTR ETAs for stop: {stopId}", stopGroup.Stop.StopId);
				allEtas = await _etaService.FetchAllMtrEtaForStop(stopGroup.Stop.StopId);
			}
			else
			{
				_logger?.LogDebug("Fetching KMB ETAs for stop: {stopId}", stopGroup.Stop.StopId);
				allEtas = await _etaService.FetchAllKmbEtaForStop(stopGroup.Stop.StopId);
			}
			
			if (allEtas.Count == 0)
			{
				_logger?.LogInformation("No ETAs found for stop: {stopId}, full ID: {fullId}", 
				    stopGroup.Stop.StopId.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase) ? 
				        stopGroup.Stop.StopId.Substring(4) : stopGroup.Stop.StopId, 
				    stopGroup.Stop.StopId);
				return false;
			}
			
			_logger?.LogInformation("Found {count} ETAs for stop: {stopId}, full ID: {fullId}", 
			    allEtas.Count, 
			    stopGroup.Stop.StopId.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase) ? 
			        stopGroup.Stop.StopId.Substring(4) : stopGroup.Stop.StopId, 
			    stopGroup.Stop.StopId);
			
			// Filter ETAs to prioritize service type "1" when multiple service types exist for a route
			// First group by route number to find duplicates
			var etasByRouteNumber = allEtas.GroupBy(e => e.RouteNumber);
			var filteredEtas = new List<TransportEta>();
			
			foreach (var routeGroup in etasByRouteNumber)
			{
				// If we have multiple service types for the same route and ETA time
				var serviceTypeGroups = routeGroup.GroupBy(e => e.EtaTime);
				
				foreach (var timeGroup in serviceTypeGroups)
				{
					// Check if we have more than one service type for this time
					if (timeGroup.Count() > 1)
					{
						// Try to find service type "1" first
						var type1Eta = timeGroup.FirstOrDefault(e => e.ServiceType == "1");
						if (type1Eta != null)
						{
							// Only add service type "1"
							filteredEtas.Add(type1Eta);
						}
						else
						{
							// If no service type "1", add the first one
							filteredEtas.Add(timeGroup.First());
						}
					}
					else
					{
						// Just one service type for this time, add it
						filteredEtas.Add(timeGroup.First());
					}
				}
			}
			
			// Apply the filtered ETAs to the stop group
			bool result = await ApplyEtasToStopGroupAsync(stopGroup, filteredEtas);
			
			// Return true if any routes in the stop group now have ETAs
			return stopGroup.Routes.Any(r => r.HasEta);
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error fetching ETAs for stop {stopName}", stopGroup.Stop.LocalizedName);
			return false;
		}
	}

	// Call this method after loading route data
	private async Task UpdateEtasForCurrentView(ObservableRangeCollection<object> stopGroupsToProcessFromCaller)
	{
		try
		{
			if (_isUpdatingEtas)
			{
				_logger?.LogDebug("Skipping ETA update as one is already in progress");
				return;
			}
			
			lock (_etaLock)
			{
				if (_isUpdatingEtas) return;
				_isUpdatingEtas = true;
			}
			
			// Store initial state of routes count - if we have items, don't show progress indicator
			bool hasExistingRoutes = Routes != null && Routes.Count > 0;
			
			// Only show refresh indicator if we don't have any routes
			if (!hasExistingRoutes)
			{
				await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = true);
			}
			
			List<StopGroup> originalStopGroups = stopGroupsToProcessFromCaller.OfType<StopGroup>().ToList();
			Dictionary<string, bool> groupActivityStatus = new Dictionary<string, bool>(); // StopId to isActive

			try
			{
				if (originalStopGroups.Count == 0)
				{
					_logger?.LogDebug("No stop groups passed to UpdateEtasForCurrentView to update ETAs for.");
					lock (_etaLock) { _isUpdatingEtas = false; } 
					// DON'T replace the Routes collection when there are no stop groups
					// Just ensure the collection is properly initialized if needed
					await MainThread.InvokeOnMainThreadAsync(() => 
					{
						// Clear refresh indicator if we set it earlier
						if (!hasExistingRoutes)
						{
							IsRefreshing = false;
						}
						
						if (this.Routes == null)
						{
							this.Routes = new ObservableRangeCollection<object>();
							var collectionView = this.FindByName<CollectionView>("RoutesCollection");
							if (collectionView != null) 
							{
								collectionView.ItemsSource = null;
								collectionView.ItemsSource = this.Routes;
							}
						}
					});
					return;
				}
				
				_logger?.LogInformation("UpdateEtasForCurrentView processing {count} stop groups passed from caller.", originalStopGroups.Count);
				
				var stopIds = originalStopGroups.Select(sg => sg.Stop.Id).Distinct().ToList();
				const int batchSize = 2; // Reduced batch size for more responsive ETA updates
				
				for (int i = 0; i < stopIds.Count; i += batchSize)
				{
					var batchStopIds = stopIds.Skip(i).Take(batchSize).ToList();
					var batchTasks = new List<Task>();
					
					foreach (var stopId in batchStopIds)
					{
						// Get all stop groups with this stop ID
						var stopGroupsWithId = originalStopGroups.Where(sg => sg.Stop.Id == stopId).ToList();
						
						if (stopGroupsWithId.Count == 0)
						{
							continue;
						}
						
						// Group stops by stop ID to avoid redundant calls
						var firstStopGroup = stopGroupsWithId.FirstOrDefault();
						if (firstStopGroup == null)
						{
							_logger?.LogWarning("No stop group found for stop ID: {stopId}", stopId);
							continue;
						}
						
						var task = Task.Run(async () =>
						{
							try
							{
								bool anySuccessfulEta = await FetchAndApplyEtasForStopGroup(firstStopGroup);
								
								// Update the activity status for this stop
								lock (groupActivityStatus)
								{
									groupActivityStatus[stopId] = anySuccessfulEta;
								}
								
								// Update other stop groups with the same ID if this is successful
								if (anySuccessfulEta && stopGroupsWithId.Count > 1)
								{
									var secondaryStopGroups = stopGroupsWithId.Skip(1).ToList();
									foreach (var secondaryGroup in secondaryStopGroups)
									{
										// Share ETAs across all stop groups with same stop ID to reduce API calls
										await MainThread.InvokeOnMainThreadAsync(() => {
											if (secondaryGroup.Routes != null)
											{
												foreach (var route in secondaryGroup.Routes)
												{
													// Find matching route in first stop group
													var sourceRoute = firstStopGroup.Routes.FirstOrDefault(r => 
														r.RouteNumber == route.RouteNumber && 
														r.Operator == route.Operator &&
														r.ServiceType == route.ServiceType);
													
													if (sourceRoute != null && sourceRoute.HasEta)
													{
														route.FirstEta = sourceRoute.FirstEta;
														route.NextEta = sourceRoute.NextEta;
														route.Etas = sourceRoute.Etas.ToList(); // Clone to avoid reference issues
													}
												}
											}
										});
									}
								}
							}
							catch (Exception ex)
							{
								_logger?.LogError(ex, "Error updating ETAs for stop ID: {stopId}", stopId);
							}
						});
						
						batchTasks.Add(task);
					}
					
					// Wait for all tasks in this batch to complete before moving to next batch
					if (batchTasks.Count > 0)
					{
						await Task.WhenAll(batchTasks);
					}
					
					// Small delay between batches to avoid UI freezing
					await Task.Delay(100);
				}
				
				// Check if we need to clean up any stop groups without ETAs
				if (_currentDistanceFilter != DistanceFilter.All)
				{
					await CleanupEmptyStopGroupsFromViewAsync();
				}
				
				// IMPORTANT: Do NOT replace the entire collection with a new one 
				// as that causes scrolling to reset and UI flickering
				
				// Instead, ensure the UI is updated in-place
				await MainThread.InvokeOnMainThreadAsync(() => {
					// Clear refresh indicator if we set it earlier
					if (!hasExistingRoutes)
					{
						IsRefreshing = false;
					}
					
					// Just notify that the collection may have changed
					OnPropertyChanged(nameof(Routes));
				});
			}
			catch (Exception ex)
			{
				_logger?.LogError(ex, "Error updating ETAs for current view");
			}
			finally
			{
				// Make sure to clear the refresh indicator if we set it
				if (!hasExistingRoutes)
				{
					await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
				}
				
				lock (_etaLock)
				{
					_isUpdatingEtas = false;
				}
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Unexpected error in UpdateEtasForCurrentView");
			
			// Clear refresh indicator in case of error
			await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
			
			lock (_etaLock) { _isUpdatingEtas = false; }
		}
	}

	// New method to cleanup empty stop groups from the UI
	private async Task CleanupEmptyStopGroupsFromViewAsync()
	{
		await MainThread.InvokeOnMainThreadAsync(() =>
		{
			if (Routes is ObservableRangeCollection<object> currentRoutesCollection)
			{
				_logger?.LogInformation("[MainThread Cleanup] Inspecting StopGroups before removal. Total items in Routes: {TotalCount}", currentRoutesCollection.Count);
				
				var groupsToRemove = currentRoutesCollection.OfType<StopGroup>()
												.Where(sg => !sg.HasAnyVisibleRoutesWithEta) // Use the property as intended
												.ToList();
				
				if (groupsToRemove.Any())
				{
					_logger?.LogInformation("[MainThread Cleanup] Removing {count} empty StopGroups with no active routes.", groupsToRemove.Count);
					
					// Use batch operations to avoid UI flickering and maintain scroll position
					currentRoutesCollection.BeginBatch();
					try {
						// Remove the groups with no active routes
						currentRoutesCollection.RemoveRange(groupsToRemove);
					}
					finally {
						// End batch mode to apply changes at once, this preserves scroll position better
						currentRoutesCollection.EndBatch();
					}
					
					// IMPORTANT: Don't call OnPropertyChanged(nameof(Routes)) as it resets scroll position
				}
				else
				{
					_logger?.LogInformation("[MainThread Cleanup] No StopGroups to remove.");
				}
			}
		});
	}

	// Add this method immediately after or before FetchEtaForStopGroup method
	/// <summary>
	/// Starts the timer for periodic location updates
	/// </summary>
	private void StartLocationUpdateTimer()
	{
		try
		{
			// Create a timer if it doesn't exist
			if (_locationUpdateTimer == null)
			{
				_locationUpdateTimer = Dispatcher.CreateTimer();
				// Increase interval to reduce location updates and prevent UI freezing
				_locationUpdateTimer.Interval = TimeSpan.FromSeconds(LocationRefreshIntervalSeconds); // Use constant value for consistency
				_locationUpdateTimer.Tick += LocationUpdateTimer_Tick;
				
				_logger?.LogInformation("Created location update timer with interval {0} seconds", _locationUpdateTimer.Interval.TotalSeconds);
			}
			
			// Start the timer if it exists and is not running, with a delay
			if (_locationUpdateTimer != null && !_locationUpdateTimer.IsRunning)
			{
				// Important: Delay the first execution to avoid UI freezes during initial load
				Task.Delay(5000).ContinueWith(_ => {
					MainThread.BeginInvokeOnMainThread(() => {
						if (_locationUpdateTimer != null && !_locationUpdateTimer.IsRunning)
						{
							_locationUpdateTimer.Start();
							_logger?.LogInformation("Started location update timer with interval {0} seconds", _locationUpdateTimer.Interval.TotalSeconds);
						}
					});
				});
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error starting location update timer");
		}
	}

	/// <summary>
	/// Handler for location update timer ticks
	/// </summary>
	private void LocationUpdateTimer_Tick(object? sender, EventArgs e)
	{
		try
		{
			_logger?.LogDebug("Location update timer tick - scheduling location update");
			
			// Run on a background thread with a small random delay to avoid UI blocking
			Task.Run(async () => {
				try
				{
					// Add some randomization to avoid all clients hitting the geolocation at once
					int randomDelay = new Random().Next(100, 500);
					await Task.Delay(randomDelay);
					
					// Actual location update work
					await UpdateUserLocationAndRefreshDistances();
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error in background location update");
				}
			});
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in location timer tick handler");
		}
	}
	
	/// <summary>
	/// Starts the timer for updating ETA display text without refetching data
	/// </summary>
	private void StartEtaDisplayUpdateTimer()
	{
		try
		{
			// Create a timer if it doesn't exist
			if (_etaDisplayUpdateTimer == null)
			{
				_etaDisplayUpdateTimer = Dispatcher.CreateTimer();
				_etaDisplayUpdateTimer.Interval = TimeSpan.FromSeconds(15); // 15 seconds refresh
				_etaDisplayUpdateTimer.Tick += EtaDisplayUpdateTimer_Tick;
				
				_logger?.LogInformation("Created ETA display update timer with interval {0} seconds", _etaDisplayUpdateTimer.Interval.TotalSeconds);
			}
			
			// Start the timer if it exists and is not running
			if (_etaDisplayUpdateTimer != null && !_etaDisplayUpdateTimer.IsRunning)
			{
				// Delay the first execution to avoid UI freezes during initial load
				Task.Delay(2000).ContinueWith(_ => {
					MainThread.BeginInvokeOnMainThread(() => {
						if (_etaDisplayUpdateTimer != null && !_etaDisplayUpdateTimer.IsRunning)
						{
							_etaDisplayUpdateTimer.Start();
							_logger?.LogInformation("Started ETA display update timer");
						}
					});
				});
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error starting ETA display update timer");
		}
	}
	
	/// <summary>
	/// Timer tick handler for updating ETA display text without fetching new data
	/// </summary>
	private void EtaDisplayUpdateTimer_Tick(object? sender, EventArgs e)
	{
		try
		{
			// Skip if another ETA update is already in progress
			if (_isUpdatingEtas || _isUpdatingEtaDisplay)
			{
				return;
			}
			
			_isUpdatingEtaDisplay = true;
			
			// Run on a background thread to avoid blocking UI
			Task.Run(async () => {
				try
				{
					await UpdateEtaDisplayText();
				}
				finally
				{
					_isUpdatingEtaDisplay = false;
				}
			});
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in ETA display update timer tick");
			_isUpdatingEtaDisplay = false;
		}
	}
	
	/// <summary>
	/// Updates the ETA display text based on current time without making API calls
	/// </summary>
	private async Task UpdateEtaDisplayText()
	{
		try
		{
			// Only process if we're showing nearby routes (distance filter)
			if (_currentDistanceFilter == DistanceFilter.All || Routes == null)
			{
				return;
			}
			
			bool anyUpdates = false;
			
			// Get all visible stop groups
			var stopGroups = Routes.OfType<StopGroup>().ToList();
			if (!stopGroups.Any())
			{
				return;
			}
			
			// Current time to compare against
			DateTime now = DateTime.Now;
			
			// Process each stop group and its routes
			foreach (var stopGroup in stopGroups)
			{
				if (stopGroup.Routes == null) continue;
				
				foreach (var route in stopGroup.Routes)
				{
					if (!route.HasEta || route.NextEta == null) continue;
					
					// Calculate new display text based on current time
					string oldDisplayText = route.FirstEta;
					
					// Update the display text based on current time
					if (route.NextEta.EtaTime != DateTime.MinValue)
					{
						TimeSpan timeDiff = route.NextEta.EtaTime - now;
						
						// Generate new display text
						string newDisplayText;
						
						if (timeDiff.TotalMinutes <= 0)
						{
							// Bus has already arrived or passed
							newDisplayText = "Arrived";
						}
						else if (timeDiff.TotalMinutes < 1)
						{
							// Less than a minute away
							newDisplayText = "Arriving";
						}
						else
						{
							int minutes = (int)Math.Floor(timeDiff.TotalMinutes);
							newDisplayText = $"{minutes} min";
						}
						
						// Only update if text has changed
						if (newDisplayText != oldDisplayText)
						{
							route.FirstEta = newDisplayText;
							anyUpdates = true;
						}
					}
				}
			}
			
			// If any updates were made, log it
			if (anyUpdates)
			{
				_logger?.LogDebug("Updated ETA display text for routes without fetching new data");
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error updating ETA display text");
		}
	}

	/// <summary>
	/// Updates user location and refreshes distances for all stops
	/// </summary>
	private async Task UpdateUserLocationAndRefreshDistances()
	{
		_logger.LogInformation("Attempting to update user location and refresh distances.");
		Location? previousUserLocation = _userLocation; // Store previous location before update

		// The new dictionary _cachedRawStopsByCenterGeoHash6 is NOT cleared here.
		// It persists and accumulates entries for different GeoHash6 areas as the user moves.
		// Old entries will remain but won't be used if the user is not in that GeoHash6 area.

		await UpdateUserLocationIfNeeded(); // This method updates _userLocation internally
		// Determine if location was actually updated by comparing with previous state
		bool locationUpdated = (_userLocation != null && !EqualityComparer<Location>.Default.Equals(previousUserLocation, _userLocation)) || 
		                     (_userLocation != null && previousUserLocation == null);

		try
		{
			if (locationUpdated)
			{
				_logger?.LogDebug("User location changed significantly, refreshing distances");
				
				// Update nearby stops with new location
				await UpdateNearbyStops();
				
				// Apply the current distance filter to refresh the list
				await ApplyDistanceFilterAsync(_currentDistanceFilter, _currentDistanceFilter);
			}
			else
			{
				_logger?.LogDebug("Location update: User hasn't moved significantly, skipping refresh");
				// Still update the timestamp to avoid too frequent checks
				_lastLocationUpdate = DateTime.Now;
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error updating location and refreshing distances");
		}
	}

	/// <summary>
	/// Updates the distances for all currently visible StopGroup items
	/// </summary>
	private void UpdateDistancesForVisibleStopGroups(Location newLocation)
	{
		try
		{
			// Save current scroll position before updating
			SaveScrollPosition();
			
			// Get all stop groups from the current view
			var stopGroups = Routes.OfType<StopGroup>().ToList();
			if (stopGroups.Count == 0)
			{
				_logger?.LogDebug("No StopGroups found to update distances");
				return;
			}
			
			_logger?.LogDebug("Updating distances for {count} stop groups", stopGroups.Count);
			
			foreach (var stopGroup in stopGroups)
			{
				if (stopGroup.Stop != null && stopGroup.Stop.Latitude != 0 && stopGroup.Stop.Longitude != 0)
				{
					// Calculate new distance
					var stopLocation = new Location(stopGroup.Stop.Latitude, stopGroup.Stop.Longitude);
					double newDistanceInMeters = Location.CalculateDistance(
						newLocation, stopLocation, DistanceUnits.Kilometers) * 1000;
					
					// The Stop.UpdateDistance method itself checks if the distance has changed significantly
					// before triggering PropertyChanged notifications, which helps prevent UI flickering
					stopGroup.UpdateDistance(newDistanceInMeters);
				}
			}
			
			// Restore scroll position to prevent jumps
			MainThread.BeginInvokeOnMainThread(async () => 
			{
				await Task.Delay(10); // Small delay to ensure UI has processed updates
				await RestoreScrollPosition();
			});
			
			_logger?.LogDebug("Finished updating distances for stop groups");
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error updating distances for visible stop groups");
		}
	}

	// Optimize distance filter changes by minimizing object allocations
	private ObservableRangeCollection<object> CreateFilteredCollection(IEnumerable<object> items)
	{
		// If we're passed null or an empty collection, return new empty collection
		if (items == null || !items.Any())
		{
			return new ObservableRangeCollection<object>();
		}
		
		// Create a new observable range collection and add all items at once
		var result = new ObservableRangeCollection<object>();
		result.AddRange(items);
		return result;
	}

	// Handle remaining items threshold reached
	private void OnRemainingItemsThresholdReached(object sender, EventArgs e)
	{
		// This event is fired when the user scrolls near the end of the list
		// It's a good place to load more data if needed
		_logger?.LogDebug("Remaining items threshold reached, ensuring all items are loaded");
	}

	private async Task PopulateRoutesInBatchesAsync(IEnumerable<object> itemsToAdd, string currentSearchQueryForKeyboard)
	{
		if (itemsToAdd == null) 
		{
			await MainThread.InvokeOnMainThreadAsync(() => {
				this.Routes = new ObservableRangeCollection<object>(); // Assign new empty collection
				KeyboardManager.UpdateAvailableRouteChars(new List<TransportRoute>(), currentSearchQueryForKeyboard);
			});
			return;
		}

		// Get all items as a list to know the total count
		List<object> itemList = itemsToAdd.ToList();
		
		// Use optimized keyboard update to avoid UI freezes
		var transportRoutes = itemList.OfType<TransportRoute>().ToList();
		_ = Task.Run(async () => await UpdateKeyboardManagerAsync(transportRoutes, currentSearchQueryForKeyboard));
		
		// Create a new observable collection
		var newCollection = new ObservableRangeCollection<object>();
		
		// Check if we have a large number of items
		if (itemList.Count > 300 && _currentDistanceFilter == DistanceFilter.All)
		{
			// For large lists in ALL view, use optimized loading
			const int initialBatchSize = 100; // Reduced from 300 to improve initial load speed
			const int subsequentBatchSize = 200;
			
			// First load an initial batch
			var initialItems = itemList.Take(initialBatchSize).ToList();
			
			// Use SuspendNotifications for better performance
			newCollection.BeginBatch();
			newCollection.AddRange(initialItems);
			newCollection.EndBatch();
			
			// Assign collection to Routes immediately for fast UI response
			await MainThread.InvokeOnMainThreadAsync(() => {
				this.Routes = newCollection;
			});
			
			// Load remaining items in the background with priority management
			_ = Task.Run(async () => 
			{
				try 
				{
					// Delay to ensure UI has updated
					await Task.Delay(200);
					
					// Process remaining items in batches
					for (int i = initialBatchSize; i < itemList.Count; i += subsequentBatchSize)
					{
						// Get the current batch
						var currentBatch = itemList
							.Skip(i)
							.Take(Math.Min(subsequentBatchSize, itemList.Count - i))
							.ToList();
						
						// Add batch to UI
						await MainThread.InvokeOnMainThreadAsync(() => 
						{
							// Use batch operations to avoid multiple UI updates
							newCollection.BeginBatch();
							newCollection.AddRange(currentBatch);
							newCollection.EndBatch();
							
							if (i % (subsequentBatchSize * 5) == 0 || i + currentBatch.Count >= itemList.Count) {
								_logger?.LogDebug("Added batch of {count} items, total now {total}/{totalItems}", 
									currentBatch.Count, newCollection.Count, itemList.Count);
							}
						});
						
						// Give UI thread time to handle user input and rendering
						await Task.Delay(50);
					}
				}
				catch (Exception ex)
				{
					_logger?.LogError(ex, "Error loading routes in batches");
				}
			});
		}
		else
		{
			// For smaller lists, optimize with batch operations
			newCollection.BeginBatch();
			newCollection.AddRange(itemList);
			newCollection.EndBatch();
			
			// Assign the populated collection to update the UI
			await MainThread.InvokeOnMainThreadAsync(() => {
				this.Routes = newCollection;
			});
		}
	}

	// Handle threshold reached for the "All" routes collection
	private void OnAllRoutesRemainingItemsThresholdReached(object? sender, EventArgs e)
	{
		// Only add more items if we're in the ALL view and showing routes
		if (_currentDistanceFilter != DistanceFilter.All || 
			!IsAllViewVisible || 
			Routes == null)
		{
			return;
		}
		
		// Get the current count of items
		int currentCount = Routes.Count;
		
		// If we already have all routes, no need to do anything
		if (currentCount >= _allRoutes.Count)
		{
			return;
		}
		
		// If we don't have all routes yet, load all remaining routes at once
		MainThread.BeginInvokeOnMainThread(() =>
		{
			// Load all remaining routes at once
			_ = Task.Run(async () =>
			{
				// Get all remaining routes
				var remainingRoutes = _allRoutes.Skip(currentCount)
											   .Cast<object>()
											   .ToList();
				
				await MainThread.InvokeOnMainThreadAsync(() =>
				{
					// Add all remaining routes at once
					foreach (var route in remainingRoutes)
					{
						Routes.Add(route);
					}
					
					_logger?.LogDebug("Added all {count} remaining routes at once", remainingRoutes.Count);
				});
			});
		});
	}

	// New, highly optimized method to load all routes with minimal UI freezing
	private async Task LoadAllRoutesWithVirtualization()
	{
		try
		{
			// Show small loading indicator in the status bar if possible
			IsBusy = true;
			
			// First, find the RoutesCollection and prepare it for optimal performance
			var routesCollection = this.FindByName<CollectionView>("RoutesCollection");
			
			// Start with an empty observable collection
			var newCollection = new ObservableRangeCollection<object>();
			
			// Use batch mode to suppress UI updates more aggressively
			newCollection.BeginBatch();
			
			// Use adaptive batch sizing based on device capabilities
			int initialBatchSize = 20;  // Smaller initial batch for immediate response
			
			// Get device screen info on the main thread
			double screenPixels = 0;
			await MainThread.InvokeOnMainThreadAsync(() => {
				var displayInfo = DeviceDisplay.MainDisplayInfo;
				screenPixels = displayInfo.Width * displayInfo.Height;
			});
			
			// Determine optimal batch size based on device capabilities
			// Higher-resolution devices likely have more memory and processing power
			bool isHighEndDevice = screenPixels > 2000000; // Over 2 million pixels (roughly 1080p+)
			
			int batchSize = isHighEndDevice ? 40 : 20; // 40 for high-res devices, 20 for lower-res
			
			// Also adjust delay based on device performance
			int batchDelay = isHighEndDevice ? 20 : 35; // Shorter delay for high-end devices
			
			_logger?.LogDebug("Using adaptive batch size: {0} items with {1}ms delay based on device capabilities",
				batchSize, batchDelay);
			
			// Make a copy of the routes to avoid database contention during iteration
			// We'll take a snapshot of the current routes
			List<TransportRoute> routesSnapshot;
			
			if (_allRoutes == null || _allRoutes.Count == 0)
			{
				// If routes aren't loaded yet, load a small set to show something
				routesSnapshot = new List<TransportRoute>();
				_logger?.LogWarning("Routes not loaded, showing empty view");
				await MainThread.InvokeOnMainThreadAsync(() => Routes = newCollection);
				IsBusy = false;
				return;
			}
			else 
			{
				// Use a thread-safe copy to avoid collection modification issues
				lock (_allRoutes)
				{
					routesSnapshot = new List<TransportRoute>(_allRoutes);
				}
			}
			
			// Add first batch immediately for quick feedback
			var initialBatch = routesSnapshot.Take(initialBatchSize).ToList();
			
			// First update - just show the initial batch
			newCollection.AddRange(initialBatch);
			
			// End batch operation to show first batch of items
			newCollection.EndBatch();
			
			// Update Routes property with initial batch
			await MainThread.InvokeOnMainThreadAsync(() => 
			{
				Routes = newCollection;
				
				// Update keyboard for filtering
				KeyboardManager.UpdateAvailableRouteChars(_allRoutes, _searchQuery);
				
				_logger?.LogInformation("Loaded initial {count}/{total} routes for ALL view", 
					initialBatchSize, routesSnapshot.Count);
			});
			
			// IMPORTANT: Let UI breathe before continuing
			await Task.Delay(150);
			await Task.Yield();
			
			// If that's all the items, we're done
			if (initialBatchSize >= routesSnapshot.Count)
			{
				IsBusy = false;
				return;
			}
			
			// Track memory usage thresholds to dynamically adapt to device conditions
			long startMemory = GC.GetTotalMemory(false);
			bool hasTriggeredGC = false;
			
			// Process in very small batches with delays between each
			for (int offset = initialBatchSize; offset < routesSnapshot.Count; offset += batchSize)
			{
				// Check if memory usage has increased significantly, and if so, force GC
				long currentMemory = GC.GetTotalMemory(false);
				if (!hasTriggeredGC && currentMemory > startMemory * 1.5)
				{
					// If memory has increased by 50%, trigger a garbage collection
					GC.Collect(GC.MaxGeneration, GCCollectionMode.Optimized, false);
					hasTriggeredGC = true;
					
					// Give GC time to complete without blocking UI
					await Task.Delay(50);
					
					// Reset start memory reference point
					startMemory = GC.GetTotalMemory(false);
				}
				
				// Start a new batch 
				newCollection.BeginBatch();
				
				// Get current batch efficiently
				var currentBatch = routesSnapshot
					.Skip(offset)
					.Take(Math.Min(batchSize, routesSnapshot.Count - offset))
					.ToList();
				
				// Process items directly
				newCollection.AddRange(currentBatch);
				
				// End current batch to apply changes
				newCollection.EndBatch();
				
				// Log progress less frequently to reduce log spam
				if (offset % (batchSize * 5) == 0 || offset + currentBatch.Count >= routesSnapshot.Count)
				{
					_logger?.LogDebug("Added batch {current}/{total}, processing {count}/{allCount} routes", 
						(offset - initialBatchSize) / batchSize + 1, 
						(int)Math.Ceiling((routesSnapshot.Count - initialBatchSize) / (double)batchSize),
						offset + currentBatch.Count, routesSnapshot.Count);
				}
				
				// CRITICAL: Explicitly yield control back to UI thread 
				await Task.Delay(batchDelay);
				await Task.Yield();
				
				// Force garbage collection occasionally to prevent memory pressure
				if (offset % 500 == 0)
				{
					GC.Collect(GC.MaxGeneration, GCCollectionMode.Optimized, false);
				}
			}
			
			// Final notification
			await MainThread.InvokeOnMainThreadAsync(() =>
			{
				_logger?.LogInformation("Completed loading all {count} routes for ALL view", newCollection.Count);
				IsBusy = false;
			});
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in LoadAllRoutesWithVirtualization");
			IsBusy = false;
		}
	}

	// After the UpdateDistancesForVisibleStopGroups method, add this CollectionView optimization method
	private static void OptimizeCollectionView(CollectionView collectionView)
	{
		if (collectionView == null) return;
		
		// Set items updating scroll mode to keep position
		collectionView.ItemsUpdatingScrollMode = ItemsUpdatingScrollMode.KeepScrollOffset;
		
		// Reduce prefetch to minimize memory usage while still ensuring smooth scrolling
		collectionView.RemainingItemsThreshold = 3;
		
		// Disable constrained layout for better performance
		#if WINDOWS
		Microsoft.Maui.Controls.PlatformConfiguration.WindowsSpecific.CollectionView.SetConstrainLayout(
			collectionView, false);
		#endif
	}

	// Optimize keyboard management to avoid UI locks during filtering
	private async Task UpdateKeyboardManagerAsync(List<TransportRoute> routes, string searchQuery)
	{
		// First, process the data on a background thread
		Dictionary<string, bool> enabledKeys = new Dictionary<string, bool>();
		
		await Task.Run(() => {
			HashSet<string> availableChars = new HashSet<string>();
			
			// Calculate available characters in background
			if (string.IsNullOrEmpty(searchQuery))
			{
				// Use globally available characters when no search
				foreach (var route in routes)
				{
					if (route != null && !string.IsNullOrEmpty(route.RouteNumber))
					{
						foreach (char c in route.RouteNumber)
						{
							availableChars.Add(c.ToString().ToUpperInvariant());
						}
					}
				}
			}
			else
			{
				// Filter for next possible character based on search
				foreach (var route in routes)
				{
					if (route == null || string.IsNullOrEmpty(route.RouteNumber)) continue;
					
					string routeNum = route.RouteNumber;
					
					// Only consider route if it starts with search query
					if (routeNum.StartsWith(searchQuery, StringComparison.OrdinalIgnoreCase))
					{
						// Only add next character after search query
						if (searchQuery.Length < routeNum.Length)
						{
							availableChars.Add(routeNum[searchQuery.Length].ToString().ToUpperInvariant());
						}
					}
				}
			}
			
			// Prepare the enabled keys map
			foreach (char digit in "0123456789")
			{
				string digitStr = digit.ToString();
				enabledKeys[digitStr] = availableChars.Contains(digitStr);
			}
			
			foreach (char c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
			{
				string key = c.ToString();
				enabledKeys[key] = availableChars.Contains(key);
			}
		});
		
		// Then apply the changes on the UI thread
		await MainThread.InvokeOnMainThreadAsync(() => {
			KeyboardManager.UpdateAvailableRouteChars(routes, searchQuery);
		});
	}

	// Modify the FetchETAsForStop method to use optimized ETA fetching
	private async Task FetchETAsForStop(TransportStop stop)
	{
		if (stop == null || string.IsNullOrEmpty(stop.Id))
		{
			_logger?.LogWarning("Cannot fetch ETAs for null stop");
			return;
		}
		
		try
		{
			_logger?.LogDebug("Fetching ETAs for stop: {stopId} ({stopName})", stop.Id, stop.Name);
			
					// Check if it's an MTR stop or KMB stop
		List<TransportEta> etas;
		
		if (stop.Id.StartsWith("MTR-", StringComparison.OrdinalIgnoreCase))
			{
				_logger?.LogDebug("Fetching MTR ETAs for stop: {stopId}", stop.Id);
				etas = await _etaService.FetchAllMtrEtaForStop(stop.Id);
			}
			else
			{
				_logger?.LogDebug("Fetching KMB ETAs for stop: {stopId}", stop.Id);
				etas = await _etaService.FetchAllKmbEtaForStopOptimized(stop.Id);
			}
			
			if (etas.Count == 0)
			{
				_logger?.LogDebug("No ETAs found for stop: {stopId}", stop.Id);
				return;
			}
			
			_logger?.LogDebug("Found {count} ETAs for stop: {stopId}", etas.Count, stop.Id);
			
			// Get all routes for this stop from the database
			var routes = await _databaseService.GetRoutesForStopAsync(stop.Id);
			
			// Group ETAs by route
			var etasByRoute = etas.GroupBy(e => e.RouteId).ToDictionary(g => g.Key, g => g.ToList());
			
			// Update the UI with ETAs for each route
			int routesWithEtas = 0;
			
			foreach (var route in routes)
			{
				if (etasByRoute.TryGetValue(route.Id, out var routeEtas))
				{
					routesWithEtas++;
					
					// Update the route's ETAs
					UpdateRouteEtas(route, routeEtas);
				}
			}
			
			_logger?.LogDebug("Updated ETAs for {routesWithEtas}/{totalRoutes} routes at stop {stopId}", 
				routesWithEtas, routes.Count, stop.Id);
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error fetching ETAs for stop {stopId}: {message}", stop.Id, ex.Message);
		}
	}

	// Add a helper method to update route ETAs
	private void UpdateRouteEtas(TransportRoute route, List<TransportEta> etas)
	{
		if (route == null || etas == null || !etas.Any())
			return;
		
		// Update in UI thread
		MainThread.BeginInvokeOnMainThread(() =>
		{
			try
			{
				// Update the route with ETAs in a safe way
				route.NextEta = etas.OrderBy(e => e.EtaTime).FirstOrDefault();
				route.Etas = new List<TransportEta>(etas);
				
				// Force UI refresh for this route
				route.OnPropertyChanged(nameof(route.NextEta));
				route.OnPropertyChanged(nameof(route.FirstEtaText));
				route.OnPropertyChanged(nameof(route.FirstEtaMinutes));
				route.OnPropertyChanged(nameof(route.HasEtas));
			}
			catch (Exception ex)
			{
				_logger?.LogError(ex, "Error updating route ETAs in UI: {message}", ex.Message);
			}
		});
	}

	// Add new method for displaying stops without waiting for ETAs
	private async Task<ObservableRangeCollection<object>> ApplyDistanceFilterWithoutEtasAsync(DistanceFilter filter)
	{
		try
		{
			// If user location is null or we're showing all routes, just use standard filtering
			if (_userLocation == null || filter == DistanceFilter.All)
			{
				var resultsList = FilterRoutesInternal();
				var result = new ObservableRangeCollection<object>();
				result.BeginBatch();
				result.AddRange(resultsList);
				result.EndBatch();
				return result;
			}

			// Get the list of nearby stops for the selected distance
			List<TransportStop> nearbyStops = new List<TransportStop>();
			string distanceKey = "";
			switch (filter)
			{
				case DistanceFilter.Meters100: distanceKey = "100m"; break;
				case DistanceFilter.Meters200: distanceKey = "200m"; break;
				case DistanceFilter.Meters400: distanceKey = "400m"; break;
				case DistanceFilter.Meters600: distanceKey = "600m"; break;
			}
			if (!string.IsNullOrEmpty(distanceKey) && _stopsNearby.ContainsKey(distanceKey))
			{
				nearbyStops = _stopsNearby[distanceKey];
			}
			ObservableRangeCollection<object> filteredItems = new ObservableRangeCollection<object>();
			if (nearbyStops.Count == 0)
			{
				return filteredItems;
			}
			nearbyStops = nearbyStops.OrderBy(s => s.DistanceFromUser).ToList();
			var stopGroups = new List<StopGroup>();
			// --- BATCH ROUTE QUERY ---
			var stopIds = nearbyStops.Select(stop => stop.Id).ToList();
			var routesByStopId = await _databaseService.GetRoutesForStopsAsync(stopIds);
			foreach (var stop in nearbyStops)
			{
				var routesForStop = routesByStopId.ContainsKey(stop.Id) ? routesByStopId[stop.Id] : new List<TransportRoute>();
				if (!string.IsNullOrEmpty(_searchQuery))
				{
					routesForStop = routesForStop.Where(r => r.RouteNumber.Contains(_searchQuery, StringComparison.OrdinalIgnoreCase)).ToList();
				}
				if (routesForStop.Any())
				{
					var stopGroup = new StopGroup(
						stop,
						routesForStop.OrderBy(r => r.RouteNumber).ToList(),
						stop.DistanceFromUser);
					stopGroups.Add(stopGroup);
				}
			}
			stopGroups = stopGroups.OrderBy(sg => sg.DistanceInMeters).ToList();
			filteredItems.BeginBatch();
			foreach (var group in stopGroups)
			{
				filteredItems.Add(group);
			}
			filteredItems.EndBatch();
			_logger?.LogInformation("Displaying {count} stop groups without ETAs", stopGroups.Count);
			return filteredItems;
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in ApplyDistanceFilterWithoutEtasAsync: {message}", ex.Message);
			return new ObservableRangeCollection<object>();
		}
	}

	// Helper to get stops for a specific DistanceFilter from _stopsNearby
	private List<TransportStop> GetStopsForDistanceFilter(DistanceFilter filter)
	{
		string distanceKey = "";
		switch (filter)
		{
			case DistanceFilter.Meters100: distanceKey = "100m"; break;
			case DistanceFilter.Meters200: distanceKey = "200m"; break;
			case DistanceFilter.Meters400: distanceKey = "400m"; break;
			case DistanceFilter.Meters600: distanceKey = "600m"; break;
			case DistanceFilter.All: 
				_logger?.LogWarning("GetStopsForDistanceFilter called with ALL filter. This method is for nearby stops only.");
				return new List<TransportStop>(); 
		}
		return _stopsNearby.TryGetValue(distanceKey, out var stops) ? stops : new List<TransportStop>();
	}

	private async Task AdaptDisplayedStopsToFilterAsync(DistanceFilter newFilter)
	{
		await _adaptLock.WaitAsync();
		try
		{
			// Check if we have existing routes to decide whether to show refresh indicator
			bool hasExistingItems = Routes != null && Routes.Count > 0;
			
			// Only show refresh indicator for empty lists to prevent shifting/flickering
			if (!hasExistingItems)
			{
				await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = true);
			}
			
			_logger?.LogInformation("Adapting display from current UI state to filter {newFilter}. Previous filter value was: {prevFilterValue}", newFilter, _previousDistanceFilterForIncrementalUpdate);

			if (newFilter == DistanceFilter.All)
			{
				_logger?.LogInformation("AdaptDisplayedStopsToFilterAsync: Filter is ALL. Calling LoadAllRoutesWithVirtualization.");
				await LoadAllRoutesWithVirtualization();
				_previousDistanceFilterForIncrementalUpdate = newFilter;
				return;
			}

			if (_userLocation == null)
			{
				_logger?.LogWarning("AdaptDisplayedStopsToFilterAsync: User location is null. Clearing routes.");
				await MainThread.InvokeOnMainThreadAsync(() => this.Routes.Clear());
				_previousDistanceFilterForIncrementalUpdate = newFilter;
				return;
			}

			var currentStopGroupsOnDisplay = this.Routes.OfType<StopGroup>().ToList();
			var currentTransportStopsOnDisplay = currentStopGroupsOnDisplay.Select(sg => sg.Stop).ToList();
			var currentTransportStopIdsOnDisplay = currentTransportStopsOnDisplay.Select(s => s.Id).ToHashSet();

			var targetTransportStopsForNewFilter = GetStopsForDistanceFilter(newFilter);
			if (!targetTransportStopsForNewFilter.Any() && !currentTransportStopsOnDisplay.Any())
			{
				_logger?.LogInformation("Adapt: No target stops for new filter {newFilter} and no stops currently displayed. Clearing UI if not already empty.", newFilter);
				if (this.Routes.Any())
				{
					await MainThread.InvokeOnMainThreadAsync(() => this.Routes.Clear());
				}
				_previousDistanceFilterForIncrementalUpdate = newFilter;
				return;
			}
			
			_logger?.LogDebug("Adapt: Current UI has {count} stop groups. Target filter {newFilter} has {targetCount} stops.", currentStopGroupsOnDisplay.Count, newFilter, targetTransportStopsForNewFilter.Count);

			// Determine groups to keep (already on UI and in new filter)
			var stopGroupsToKeep = currentStopGroupsOnDisplay
				.Where(sg => targetTransportStopsForNewFilter.Any(ts => ts.Id == sg.Stop.Id))
				.ToList();
			_logger?.LogDebug("Adapt: Identified {count} StopGroups to keep from current UI.", stopGroupsToKeep.Count);

			// Determine transport stops that need to be added (in new filter but not on UI)
			var transportStopsToAddCandidates = targetTransportStopsForNewFilter
				.Where(ts => !currentTransportStopIdsOnDisplay.Contains(ts.Id))
				.ToList();
			_logger?.LogDebug("Adapt: Found {count} candidate new TransportStops to process for adding.", transportStopsToAddCandidates.Count);
			
			var newActiveStopGroupsReadyForUi = new List<StopGroup>();
			var tempInitialGroupsForNewStops = new List<StopGroup>();

			if (transportStopsToAddCandidates.Any())
			{
				var stopwatch = System.Diagnostics.Stopwatch.StartNew();
				await Task.Run(async () => // Offload database access and initial processing
				{
					var locationSnapshot = _userLocation; // Capture for thread safety
					if (locationSnapshot == null)
					{
						_logger?.LogWarning("Adapt: User location became null during background processing of new stops. Skipping.");
						return;
					}

					// Batch fetch all routes for all stops to add
					var stopIds = transportStopsToAddCandidates.Select(stop => stop.Id).ToList();
					var routesByStopId = await _databaseService.GetRoutesForStopsAsync(stopIds);

					foreach (var stop in transportStopsToAddCandidates)
					{
						var routesForStop = routesByStopId.ContainsKey(stop.Id) ? routesByStopId[stop.Id] : new List<TransportRoute>();
						if (!string.IsNullOrEmpty(_searchQuery))
						{
							routesForStop = routesForStop.Where(r => r.RouteNumber.Contains(_searchQuery, StringComparison.OrdinalIgnoreCase)).ToList();
						}
						// Deduplicate: Only keep ServiceType == "1" if multiple service types exist for the same route
						routesForStop = routesForStop
							.GroupBy(r => new { r.Operator, r.RouteNumber, r.Bound })
							.Select(g =>
							{
								if (g.Count() == 1)
									return g.First();
								var primary = g.FirstOrDefault(r => r.ServiceType == "1");
								return primary ?? g.First();
							})
							.ToList();
						if (routesForStop.Any())
						{
							double distance = Location.CalculateDistance(locationSnapshot, new Location(stop.Latitude, stop.Longitude), DistanceUnits.Kilometers) * 1000;
							tempInitialGroupsForNewStops.Add(new StopGroup(stop, routesForStop, distance));
						}
					}
				});
				stopwatch.Stop();
				_logger?.LogInformation("Adapt: DB processing for {count} new stops took {ms}ms.", transportStopsToAddCandidates.Count, stopwatch.ElapsedMilliseconds);


				if (tempInitialGroupsForNewStops.Any())
				{
					_logger?.LogDebug("Adapt: Created {count} initial StopGroups from new stops (background task). Starting ETA processing.", tempInitialGroupsForNewStops.Count);
					var etaStopwatch = System.Diagnostics.Stopwatch.StartNew();
					
					var activeGroupProcessingTasks = new List<Task<(StopGroup group, bool isActive)>>();

					foreach (var newStopGroup in tempInitialGroupsForNewStops)
                    {
                        if (newStopGroup == null) continue;

                        activeGroupProcessingTasks.Add(Task.Run(async () =>
                        {
                            // Use the correct ETA model: Models.TransportEta
                            List<Models.TransportEta> etas = new List<Models.TransportEta>();
                            if (newStopGroup.Stop.Operator == TransportOperator.KMB)
                            {
                                // Assuming FetchAllKmbEtaForStopOptimized returns List<Models.TransportEta>
                                etas = await _etaService.FetchAllKmbEtaForStopOptimized(newStopGroup.Stop.Id);
                            }
                            else if (newStopGroup.Stop.Operator == TransportOperator.MTR)
                            {
                                // Fetch MTR ETAs
                                etas = await _etaService.FetchAllMtrEtaForStop(newStopGroup.Stop.Id);
                            }
                            // Add other operators as needed

                            // Apply the fetched ETAs and get the status - using the boolean-returning version
                            bool anyRouteGotEta = await ApplyEtasToStopGroupAsync(newStopGroup, etas);
                            return (newStopGroup, anyRouteGotEta);
                        }));
                    }
					
                    var groupActivityResults = await Task.WhenAll(activeGroupProcessingTasks);

					foreach (var result in groupActivityResults)
                    {
                        // Always include MTR stops even if they don't have ETAs
                        if (result.group != null && (result.isActive || result.group.Stop.Operator == TransportOperator.MTR))
                        {
                            // For MTR stops, we want to include them even without ETAs
                            if (result.group.Stop.Operator == TransportOperator.MTR)
                            {
                                newActiveStopGroupsReadyForUi.Add(result.group);
                            }
                            // For other operators, only include if they have ETAs
                            else if (result.isActive)
                            {
                                // Get the original route collection from the StopGroup
                                var originalRoutesInGroup = result.group.Routes; // This is an ObservableCollection<TransportRoute>
                                
                                var routesWithEta = originalRoutesInGroup.Where(r => r.HasEta).ToList();
                                
                                if (routesWithEta.Any())
                                {
                                    // Update the original collection in the StopGroup
                                    originalRoutesInGroup.Clear();
                                    foreach (var routeToAdd in routesWithEta)
                                    {
                                        originalRoutesInGroup.Add(routeToAdd);
                                    }
                                    newActiveStopGroupsReadyForUi.Add(result.group);
                                }
                            }
                        }
                    }

					etaStopwatch.Stop();
					_logger?.LogInformation("Adapt: ETA processing for {NewStopGroupCount} new StopGroups took {ElapsedMs}ms. Found {ActiveCount} active groups.", tempInitialGroupsForNewStops.Count, etaStopwatch.ElapsedMilliseconds, newActiveStopGroupsReadyForUi.Count);
				}
				_logger?.LogDebug("Adapt: Prepared {count} new active StopGroups to add to UI.", newActiveStopGroupsReadyForUi.Count);
			}

			// Combine kept groups and newly added active groups
			List<StopGroup> finalStopGroupsForUi = new List<StopGroup>(stopGroupsToKeep);
			finalStopGroupsForUi.AddRange(newActiveStopGroupsReadyForUi);
			
			// Sort the final list by distance
			finalStopGroupsForUi = finalStopGroupsForUi.OrderBy(sg => sg.DistanceInMeters).ToList();
			_logger?.LogDebug("Adapt: Final combined list has {count} StopGroups before UI update.", finalStopGroupsForUi.Count);

			await MainThread.InvokeOnMainThreadAsync(() =>
			{
				// Get the current collection
				var currentCollection = this.Routes;

				// We want to preserve the same collection instance to maintain scroll position
				if (currentCollection is ObservableRangeCollection<object> observableCollection)
				{
					// Get current state of items
					var currentStopGroups = observableCollection.OfType<StopGroup>().ToList();
					var nonStopGroupItems = observableCollection.Where(item => !(item is StopGroup)).ToList();
					
					// Special case: If we're keeping all current items and just adding new ones, use AddRange to avoid scroll reset
					var currentIds = currentStopGroups.Select(sg => sg.Stop.Id).ToHashSet();
					var finalIds = finalStopGroupsForUi.Select(sg => sg.Stop.Id).ToHashSet();
					
					bool isAddingOnly = finalIds.IsSupersetOf(currentIds) && finalIds.Count > currentIds.Count;
					bool isRemovingOnly = currentIds.IsSupersetOf(finalIds) && currentIds.Count > finalIds.Count;
					bool isUnchanged = currentIds.SetEquals(finalIds) && currentIds.Count == finalIds.Count;
					bool needsReordering = finalStopGroupsForUi.Count > 0 && 
						!finalStopGroupsForUi.Select(sg => sg.Stop.Id)
						.SequenceEqual(currentStopGroups.Select(sg => sg.Stop.Id));
						
					// IMPORTANT: Use batch operations to minimize UI updates and preserve scroll position
					observableCollection.BeginBatch();
					
					try
					{
						if (isUnchanged && !needsReordering)
						{
							// Nothing to do if the collection is unchanged and in the same order
							_logger?.LogDebug("Adapt: No changes to collection needed");
						}
						else if (isAddingOnly && !needsReordering)
						{
							// Just add the new items to the end
							var newStopGroups = finalStopGroupsForUi.Where(sg => !currentIds.Contains(sg.Stop.Id)).ToList();
							_logger?.LogDebug("Adapt: Adding {count} new stop groups", newStopGroups.Count);
							observableCollection.AddRange(newStopGroups);
						}
						else if (isRemovingOnly && !needsReordering)
						{
							// Just remove items no longer needed
							var stopGroupsToRemove = currentStopGroups.Where(sg => !finalIds.Contains(sg.Stop.Id)).ToList();
							_logger?.LogDebug("Adapt: Removing {count} stop groups", stopGroupsToRemove.Count);
							observableCollection.RemoveRange(stopGroupsToRemove);
						}
						else
						{
							// More complex case - preserve any non-StopGroup items
							observableCollection.Clear();
							
							// Add all items back in the correct order
							if (nonStopGroupItems.Any())
							{
								observableCollection.AddRange(nonStopGroupItems);
							}
							
							// Add the stop groups in their sorted order
							observableCollection.AddRange(finalStopGroupsForUi);
							_logger?.LogDebug("Adapt: Complete rebuild of collection with {count} stop groups", finalStopGroupsForUi.Count);
						}
					}
					finally
					{
						// End batch to apply all changes at once
						observableCollection.EndBatch();
					}
				}
				else
				{
					// Fallback (should never happen)
					var newCollection = new ObservableRangeCollection<object>();
					newCollection.AddRange(finalStopGroupsForUi);
					this.Routes = newCollection;
					_logger?.LogWarning("Adapt: Had to create new collection - scroll position may reset");
				}
			});

			_previousDistanceFilterForIncrementalUpdate = newFilter;
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error in AdaptDisplayedStopsToFilterAsync");
		}
		finally
		{
			_adaptLock.Release();
			// Clear the refresh indicator on the UI thread to ensure it's updated
			await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
			_logger?.LogInformation("Finished adapting display to filter {newFilter}. Routes count: {count}", newFilter, this.Routes.Count);
		}
	}

	// This is a void version that doesn't return a value but modifies the stop group in-place
	private async Task ApplyEtasToStopGroupVoidAsync(StopGroup stopGroup, List<TransportEta> allEtas)
	{
		try
		{
			// Save the expanded state to restore it after updates
			bool wasExpanded = stopGroup.IsExpanded;
			
			// Only run UI updates on the main thread
			await MainThread.InvokeOnMainThreadAsync(() => {
				if (stopGroup.Routes != null && stopGroup.Routes.Count > 0)
				{
					// Keep track of how many routes have ETAs for the stop group's state
					int routesWithEta = 0;
					
					// Match the ETAs to routes inside this stop group
					foreach (var route in stopGroup.Routes)
					{
						// Find matching ETAs for this route
						var routeEtas = allEtas.Where(eta => 
							eta.RouteNumber == route.RouteNumber &&
							// TransportEta doesn't have Operator, so rely on matching RouteNumber and ServiceType
							(!string.IsNullOrEmpty(route.ServiceType) && !string.IsNullOrEmpty(eta.ServiceType) 
								? route.ServiceType.Equals(eta.ServiceType, StringComparison.OrdinalIgnoreCase) 
								: true))
							.OrderBy(eta => eta.EtaTime)
							.ToList();
							
						if (routeEtas.Count > 0)
						{
							// Update existing route ETAs in-place without recreating objects
							route.Etas.Clear();
							route.Etas.AddRange(routeEtas);
							
							// Set firstEta string and nextEta object - this will trigger HasEta property through notification
							route.FirstEta = routeEtas[0].DisplayEta;
							route.NextEta = routeEtas.Count > 1 ? routeEtas[1] : null;
							
							routesWithEta++;
						}
						else
						{
							route.Etas.Clear();
							route.FirstEta = string.Empty; // This will update HasEta property through notification
							route.NextEta = null;
						}
					}
					
					// Preserve expanded state regardless of ETA status to avoid UI jumping
					// NEVER collapse stop groups that were previously expanded, as it causes UI jumps
					// Respect the user's choice to keep groups expanded
				}
			});
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error applying ETAs to stop group");
		}
	}

	// Add method to save scroll position before ETA updates
	private void SaveScrollPosition()
	{
		if (Routes?.Count > 0)
		{
			_shouldRestoreScrollPosition = true;
			_logger?.LogDebug("Saving scroll position: FirstVisibleItem={FirstVisible}, Offset={Offset}", 
				_lastScrolledItemIndex, _lastScrolledItemOffset);
		}
		else
		{
			_shouldRestoreScrollPosition = false;
		}
	}

	// Add method to restore scroll position after ETA updates
	private async Task RestoreScrollPosition()
	{
		try
		{
			if (!_shouldRestoreScrollPosition || _lastScrolledItemIndex < 0 || RoutesCollection == null || Routes?.Count == 0)
			{
				return;
			}

			_isRestoringScrollPosition = true;
			
			try
			{
				await MainThread.InvokeOnMainThreadAsync(async () => {
					// Ensure the index is valid
					int itemIndex = Math.Min(_lastScrolledItemIndex, Routes.Count - 1);
					if (itemIndex >= 0)
					{
						_logger?.LogDebug("Restoring scroll position to item {ItemIndex}", itemIndex);
						RoutesCollection.ScrollTo(itemIndex, -1, ScrollToPosition.Start, false);
						
						// Small delay to allow the UI to update
						await Task.Delay(50);
					}
				});
			}
			finally
			{
				_isRestoringScrollPosition = false;
				_shouldRestoreScrollPosition = false;
			}
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Error restoring scroll position");
			_isRestoringScrollPosition = false;
			_shouldRestoreScrollPosition = false;
		}
	}
}

// Value converter to check if an item is a StopGroup
public class IsStopGroupConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        return value is StopGroup;
    }
    
    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}

// Value converter to check if an item is NOT a StopGroup
public class IsNotStopGroupConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        return value is not StopGroup;
    }
    
    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}

// Converter for Expand/Collapse Icon
public class ExpandCollapseIconConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is bool isExpanded)
        {
            return isExpanded ? FontAwesomeIcons.ChevronDown : FontAwesomeIcons.ChevronRight;
        }
        return FontAwesomeIcons.ChevronRight; // Default to collapsed icon
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}

// Value converter to divide values (Moved inside the namespace)
public class DivideValueConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value == null || parameter == null) return 0;
        
        double num = System.Convert.ToDouble(value);
        double divisor = System.Convert.ToDouble(parameter);
        
        if (divisor == 0) return 0;
        return num / divisor;
    }
    
    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}

// Converter to calculate appropriate height for a collection based on item count
public class CountToHeightConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        const double MinimumHeight = 66.0; // Increased minimum height for larger text
        
        if (value == null) return MinimumHeight;
        
        // If value is an int (e.g., Routes.Count)
        if (value is int count)
        {
            // Use a larger height per item for better readability
            double itemHeight = 66.0;
            
            // Always show at least one item's height, even if count is 0
            int itemsToShow = Math.Max(1, Math.Min(count, 5));
            
            // Calculate total height with minimum safeguard
            return Math.Max(MinimumHeight, itemsToShow * itemHeight);
        }
        
        return MinimumHeight; // Default minimum height
    }
    
    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}

// Converter for operator to color mapping
public class OperatorToColorConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        // Default values if binding fails
        if (value == null) return parameter?.ToString() == "Text" ? Colors.Black : Colors.LightGray;
        
        string operatorValue = value.ToString()?.ToUpperInvariant() ?? "";
        string paramType = parameter?.ToString() ?? "Background";
        
        if (paramType == "Text")
        {
            // Return text color based on operator
            return operatorValue switch
            {
                "KMB" => Color.FromArgb("#E53935"),  // Red for KMB
                "MTR" => Color.FromArgb("#9C27B0"),  // Purple for MTR
                "CTB" => Color.FromArgb("#1976D2"),  // Blue for CTB
                _ => Color.FromArgb("#C71585")       // Default magenta
            };
        }
        else
        {
            // Return background color based on operator
            return operatorValue switch
            {
                "KMB" => Color.FromArgb("#FFEBEE"),  // Light red for KMB
                "MTR" => Color.FromArgb("#F2E6FF"),  // Light purple for MTR
                "CTB" => Color.FromArgb("#E3F2FD"),  // Light blue for CTB
                _ => Color.FromArgb("#FFE1E8")       // Default light pink
            };
        }
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}

// Replace the PerformanceOptimizer class with an enhanced version
public class PerformanceOptimizer
{
    private bool _optimizationsEnabled = false;
    
    public void OptimizeForLargeDataset(bool enable)
    {
        if (_optimizationsEnabled == enable)
            return;
            
        _optimizationsEnabled = enable;
        
        // Apply platform-specific optimizations
        if (enable)
        {
            // Optimize CollectionView if available
            var window = Microsoft.Maui.Controls.Application.Current?.Windows.FirstOrDefault();
            var mainPage = window?.Page;
            
            if (mainPage != null)
            {
                // Find all CollectionViews on the page
                var collectionViews = FindAllCollectionViews(mainPage);
                foreach (var cv in collectionViews)
                {
                    // Use batch updates to reduce UI refreshes
                    cv.BatchBegin();
                    cv.BatchCommit();
                }
                
                // Force layout refresh without animations
                mainPage.BatchBegin();
                mainPage.BatchCommit();
            }
            
            #if ANDROID
            // Android-specific optimizations
            if (mainPage is NavigationPage navPage)
            {
                // Force refresh without animations
                var oldColor = navPage.BarBackgroundColor;
                navPage.BarBackgroundColor = oldColor;
            }
            #endif
        }
        else
        {
            // Restore normal operation - no specific action needed
            // as MAUI will use default animations automatically
        }
    }
    
    // Helper method to find all CollectionViews on a page
    private IEnumerable<CollectionView> FindAllCollectionViews(Element element)
    {
        var result = new List<CollectionView>();
        
        // Check if current element is a CollectionView
        if (element is CollectionView cv)
        {
            result.Add(cv);
        }
        
        // Check children recursively
        if (element is IElementController controller)
        {
            foreach (var child in controller.LogicalChildren)
            {
                if (child is Element childElement)
                {
                    result.AddRange(FindAllCollectionViews(childElement));
                }
            }
        }
        
        return result;
    }
}

// Add this class after the StopGroup class but before the MainPage class

// Simple class to show loading progress






