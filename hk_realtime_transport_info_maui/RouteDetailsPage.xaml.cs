using System.Collections.ObjectModel;
using System.Globalization;
using System.Text;
using System.Windows.Input;
using hk_realtime_transport_info_maui.Models;
using hk_realtime_transport_info_maui.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Maui.Devices.Sensors;
using NGeoHash;

namespace hk_realtime_transport_info_maui;

public partial class RouteDetailsPage : ContentPage
{
    private readonly LiteDbService _databaseService;
    private readonly EtaService _etaService;
    private readonly ILogger<RouteDetailsPage>? _logger;
    private bool _isRefreshing;
    private ObservableCollection<TransportStop> _stops = new();
    private TransportRoute? _route;
    private string _routeTitle = string.Empty;
    private string _fromLabel = string.Empty;
    private string _toLabel = string.Empty;
    private bool _isLoadingStops;
    private HtmlWebViewSource _mapSource = new();
    private TransportStop? _selectedStop;
    private bool _javaScriptHandlerRegistered;
    private Dictionary<string, List<TransportEta>> _etaData = new();
    private CancellationTokenSource? _etaRefreshCts;
    private bool _isLoadingEta;
    private IDispatcherTimer? _gpsRefreshTimer;
    private bool _isGpsRefreshActive;
    private CancellationTokenSource? _gpsRefreshCts;
    private bool _hasTwoDirections;
    
    // Timer for ETA display text updates (without fetching new ETAs)
    private IDispatcherTimer? _etaDisplayUpdateTimer;
    private bool _isUpdatingEtaDisplay = false;
    private string _favoriteIconGlyph = "\uf006"; // Star outline
    private string _favoriteIconFontFamily = "FontAwesomeRegular";
    public string FavoriteIconGlyph
    {
        get => _favoriteIconGlyph;
        set { _favoriteIconGlyph = value; OnPropertyChanged(); }
    }
    public string FavoriteIconFontFamily
    {
        get => _favoriteIconFontFamily;
        set { _favoriteIconFontFamily = value; OnPropertyChanged(); }
    }
    private bool _isFavorite;

    public ICommand FavoriteCommand { get; private set; }
    public ICommand RefreshCommand { get; }
    public ICommand ShowNearestStopCommand { get; private set; }
    public ICommand ReverseRouteCommand { get; private set; }

    public bool HasTwoDirections
    {
        get => _hasTwoDirections;
        private set
        {
            _hasTwoDirections = value;
            OnPropertyChanged();
            
            // Show/hide the reverse button based on whether route has two directions
            if (ReverseRouteButton != null)
            {
                MainThread.BeginInvokeOnMainThread(() => {
                    try
                    {
                        ToolbarItems.Remove(ReverseRouteButton);
                        if (_hasTwoDirections)
                        {
                            ToolbarItems.Add(ReverseRouteButton);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error updating reverse button visibility");
                    }
                });
            }
        }
    }

    public HtmlWebViewSource MapSource
    {
        get => _mapSource;
        set
        {
            _mapSource = value;
            OnPropertyChanged();
        }
    }

    public TransportRoute? Route 
    { 
        get => _route;
        private set
        {
            // Clean up any placeholders in the values
            if (value != null)
            {
                CleanRoutePlaceholders(value);
            }
            
            _route = value;
            
            // Explicitly notify for properties that depend on Route
            OnPropertyChanged();
            OnPropertyChanged(nameof(LocalizedOrigin));
            OnPropertyChanged(nameof(LocalizedDestination));
        }
    }

    public ObservableCollection<TransportStop> Stops
    {
        get => _stops;
        private set
        {
            _stops = value;
            OnPropertyChanged();
            UpdateMapWithStops();
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

    public string RouteTitle
    {
        get => _routeTitle;
        set
        {
            _routeTitle = value;
            OnPropertyChanged();
        }
    }

    public string FromLabel
    {
        get => _fromLabel;
        set
        {
            _fromLabel = value;
            OnPropertyChanged();
        }
    }

    public string ToLabel
    {
        get => _toLabel;
        set
        {
            _toLabel = value;
            OnPropertyChanged();
        }
    }

    // Direct access properties for cleaner binding
    public string LocalizedOrigin => Route?.LocalizedOrigin?.Replace("{0}", "").Trim() ?? string.Empty;
    
    public string LocalizedDestination => Route?.LocalizedDestination?.Replace("{0}", "").Trim() ?? string.Empty;

    public RouteDetailsPage(LiteDbService databaseService, EtaService etaService, ILogger<RouteDetailsPage>? logger)
    {
        InitializeComponent();
        _databaseService = databaseService;
        _etaService = etaService;
        _logger = logger;

        BindingContext = this;
        RefreshCommand = new Command(async () => await RefreshData());
        ShowNearestStopCommand = new Command(async () => await FindAndScrollToNearestStop());
        ReverseRouteCommand = new Command(() => {
            _logger?.LogInformation("ReverseRouteCommand executed");
            ReverseRouteDirection();
        });
        FavoriteCommand = new Command(async () => await ToggleFavorite());

        // Set up WebView events
        StopsMapView.Navigated += OnMapNavigated;
        StopsMapView.Navigating += OnMapNavigating;
        
        // Initialize the map with default HTML
        InitializeMap();
        UpdateLabels();
        
        // Initialize GPS refresh timer
        InitializeGpsRefreshTimer();
        
        // Initialize ETA display update timer
        InitializeEtaDisplayUpdateTimer();
        
        // Log for debugging the ReverseRouteButton
        _logger?.LogDebug("ReverseRouteButton initialized: {isNull}", ReverseRouteButton == null);
    }
    
    private void OnMapNavigating(object? sender, WebNavigatingEventArgs e)
    {
        try
        {
            // Handle callback URLs from JavaScript
            if (e.Url.StartsWith("callback://"))
            {
                e.Cancel = true; // Prevent actual navigation
                
                // Process the callback
                ProcessCallbackUrl(e.Url);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in OnMapNavigating");
            // Don't throw - we want to prevent app crashes
        }
    }
    
    private void ProcessCallbackUrl(string url)
    {
        try
        {
            if (url.StartsWith("callback://error"))
            {
                // Extract error message
                var uri = new Uri(url);
                var queryString = uri.Query;
                if (!string.IsNullOrEmpty(queryString) && queryString.StartsWith("?"))
                {
                    var parameters = System.Web.HttpUtility.ParseQueryString(queryString);
                    string? errorMessage = parameters["message"];
                    
                    if (!string.IsNullOrEmpty(errorMessage))
                    {
                        _logger?.LogError("JavaScript error: {message}", errorMessage);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error processing callback URL: {url}", url);
        }
    }
    
    private void OnMapNavigated(object? sender, WebNavigatedEventArgs e)
    {
        try
        {
            if (e.Result == WebNavigationResult.Success)
            {
                _logger?.LogDebug("Map successfully loaded");
                
                // Try to register JavaScript handlers right away
                MainThread.BeginInvokeOnMainThread(async () => {
                    try
                    {
                        await RegisterJavaScriptErrorHandler();
                        
                        // If there's a selected stop, navigate to it immediately
                        if (_selectedStop != null)
                        {
                            await NavigateMapToStop(_selectedStop);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error after map loaded");
                    }
                });
            }
            else
            {
                _logger?.LogWarning("Map failed to load: {result}, {error}", e.Result, e.Url);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in OnMapNavigated");
        }
    }
    
    private async Task RegisterJavaScriptErrorHandler()
    {
        if (_javaScriptHandlerRegistered)
        {
            return;
        }
        
        try
        {
            // Extremely simplified JavaScript to avoid syntax errors
            string errorHandlerScript = "window.onerror = function() { return true; }; true;";
            
            // Execute script safely
            var result = await SafeEvaluateJavaScriptAsync(errorHandlerScript);
            if (result == "true")
            {
                _javaScriptHandlerRegistered = true;
                _logger?.LogDebug("Registered JavaScript error handler");
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to register JavaScript error handler");
        }
    }

    // Handle stop selection from the CollectionView
    private void OnStopSelected(object sender, SelectionChangedEventArgs e)
    {
        if (e.CurrentSelection.FirstOrDefault() is TransportStop selectedStop)
        {
            _selectedStop = selectedStop;
            _logger?.LogDebug("Stop selected: {stopName} at position {lat}, {lng}", 
                selectedStop.LocalizedName, selectedStop.Latitude, selectedStop.Longitude);
            
            // Navigate the map to the selected stop immediately without checking initialization
            // This ensures the selection always tries to navigate, even if the map is still loading
            MainThread.BeginInvokeOnMainThread(async () => {
                try
                {
                    await NavigateMapToStop(selectedStop);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error navigating to selected stop");
                }
            });
        }
    }

    private async Task<string> SafeEvaluateJavaScriptAsync(string script)
    {
        if (string.IsNullOrEmpty(script))
        {
            return string.Empty;
        }
        
        try
        {
            // Ensure the script is properly terminated
            if (!script.EndsWith(";"))
            {
                script += ";";
            }
            
            // Add a small wrapper to catch any errors
            string wrappedScript = $"try {{ {script} }} catch(e) {{ 'error: ' + e.message; }}";
            
            var result = await StopsMapView.EvaluateJavaScriptAsync(wrappedScript);
            return result?.ToString() ?? string.Empty;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "JavaScript evaluation error");
            return string.Empty;
        }
    }

    private async Task NavigateMapToStop(TransportStop stop)
    {
        if (stop == null)
        {
            _logger?.LogWarning("Cannot navigate to null stop");
            return;
        }
        
        try
        {
            // Super simplified JavaScript with no complex syntax
            string lat = stop.Latitude.ToString("F6", CultureInfo.InvariantCulture);
            string lng = stop.Longitude.ToString("F6", CultureInfo.InvariantCulture);
            
            // Check if mapView exists
            string hasMapView = await SafeEvaluateJavaScriptAsync("window.mapView ? 'true' : 'false';");
            
            if (hasMapView == "true")
            {
                // Navigate to the stop with a simple call
                string script = $"window.mapView.goTo({{ center: [lng, lat], zoom: 17 }});";
                await SafeEvaluateJavaScriptAsync(script);
                _logger?.LogDebug("Navigated to stop at {lat}, {lng}", lat, lng);
            }
            else
            {
                _logger?.LogDebug("Map view not available yet for navigation");
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error navigating map to stop at {lat}, {lng}", 
                stop.Latitude, stop.Longitude);
        }
    }

    private void InitializeMap()
    {
        try
        {
            _javaScriptHandlerRegistered = false;
            MapSource.Html = GenerateMapHtml(new List<TransportStop>());
            _logger?.LogDebug("Map HTML initialized");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error initializing map");
        }
    }

    private void UpdateMapWithStops()
    {
        try
        {
            if (Stops.Count > 0)
            {
                _javaScriptHandlerRegistered = false;
                MapSource.Html = GenerateMapHtml(Stops.ToList());
                _logger?.LogDebug("Updated map with {count} stops", Stops.Count);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error updating map with stops");
        }
    }

    private string GetMapLanguage()
    {
        try
        {
            // Get the current culture to determine map language
            var currentCulture = CultureInfo.CurrentUICulture.Name.ToLowerInvariant();
            
            if (currentCulture.StartsWith("zh"))
            {
                // Check if it's simplified Chinese
                if (currentCulture == "zh-cn" || currentCulture == "zh-sg" || 
                    currentCulture == "zh-hans" || currentCulture.StartsWith("zh-hans-"))
                {
                    return "sc"; // Simplified Chinese
                }
                return "tc"; // Traditional Chinese (default for Chinese)
            }
            
            return "en"; // Default to English
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error determining map language, defaulting to English");
            return "en";
        }
    }

    private string GenerateMapHtml(List<TransportStop> stops)
    {
        try
        {
            // Calculate map center if there are stops
            double centerLat = 22.335269; // Default center (Hong Kong)
            double centerLng = 114.118430;
            
            // Get the map language based on the current culture
            string mapLanguage = GetMapLanguage();
            
            // Center map on first stop or calculate center from all stops
            if (stops.Count > 0)
            {
                // Calculate the center of all stops
                double totalLat = 0;
                double totalLng = 0;
                foreach (var stop in stops)
                {
                    totalLat += stop.Latitude;
                    totalLng += stop.Longitude;
                }
                centerLat = totalLat / stops.Count;
                centerLng = totalLng / stops.Count;
            }

            // Generate markers for all stops
            var markersJs = new StringBuilder();
            for (int i = 0; i < stops.Count; i++)
            {
                var stop = stops[i];
                // Ensure sequence starts from 1 for display
                int sequence = i + 1;
                
                // Get sequence from TransportStop if available (set by DatabaseService)
                var seqProperty = typeof(TransportStop).GetProperty("Sequence");
                if (seqProperty != null)
                {
                    var seqValue = seqProperty.GetValue(stop);
                    if (seqValue != null && seqValue is int seqInt && seqInt > 0)
                    {
                        sequence = seqInt;
                    }
                }
                
                // Escape any special characters in the name to prevent breaking the JavaScript
                string escapedName = stop.LocalizedName
                    .Replace("'", "\\'")
                    .Replace("\"", "\\\"");
                    
                markersJs.AppendLine($@"
                // Create a point for stop #{sequence}
                var point{i} = new Point({{
                    longitude: {stop.Longitude.ToString("F6", CultureInfo.InvariantCulture)},
                    latitude: {stop.Latitude.ToString("F6", CultureInfo.InvariantCulture)},
                    spatialReference: new SpatialReference({{ wkid: 4326 }})
                }});
                
                // Create a graphic for the marker
                var markerSymbol{i} = {{
                    type: ""simple-marker"",
                    style: ""circle"",
                    color: [50, 64, 240, 0.75],
                    outline: {{
                        color: [255, 255, 255],
                        width: 2
                    }},
                    size: 12
                }};
                
                // Add graphic to the view
                var stopGraphic{i} = new Graphic({{
                    geometry: point{i},
                    symbol: markerSymbol{i},
                    attributes: {{
                        StopId: '{stop.StopId}',
                        Name: '{escapedName}',
                        Sequence: '{sequence}'
                    }},
                    popupTemplate: {{
                        title: ""#{sequence} - {escapedName}"",
                        content: [{{
                            type: ""text"",
                            text: ""Stop ID: {stop.StopId}""
                        }}]
                    }}
                }});
                
                graphicsLayer.add(stopGraphic{i});");
            }

            // Return the complete HTML with map and markers
            return $@"<!DOCTYPE html>
<html>
<head>
    <meta charset=""utf-8"">
    <meta name=""viewport"" content=""initial-scale=1, maximum-scale=1, user-scalable=no"">
    <title>CSDI Map with Bus Stops</title>
    <style>
        html, body, #mapView {{
            padding: 0;
            margin: 0;
            height: 100%;
            width: 100%;
        }}
        .esri-attribution {{
            background-color: transparent;
        }}
        .esri-attribution__powered-by {{
            display: none;
        }}
        .esri-attribution a {{
            color: black;
        }}
        .esri-attribution {{
            background-color: transparent;
            font-size: 12px;
            font-family: sans-serif;
            color: black;
        }}
        .copyright-url {{
            position: absolute;
            bottom: 5px;
            right: 40px;
            padding: 0 4px;
            font-family: sans-serif;
            font-size: 12px;
        }}
        .copyright-logo {{
            position: absolute;
            bottom: 5px;
            right: 10px;
            width: 28px;
            height: 28px;
            display: inline-flex;
            background: url(https://api.hkmapservice.gov.hk/mapapi/landsdlogo.jpg);
            background-size: 28px;
        }}
    </style>
    <link rel=""stylesheet"" href=""https://js.arcgis.com/4.29/esri/themes/light/main.css"">
    <script src=""https://js.arcgis.com/4.29/""></script>
    <script>
        // Create global error handler - simplified to minimize syntax errors
        window.onerror = function(message, source, lineno, colno, error) {{
            console.log('Global error: ' + message);
            return true;
        }};
        
        require([
            ""esri/Map"", ""esri/Basemap"", ""esri/layers/VectorTileLayer"",
            ""esri/views/MapView"", ""esri/geometry/SpatialReference"",
            ""esri/geometry/Point"", ""esri/Graphic"", ""esri/layers/GraphicsLayer""
        ], function (Map, Basemap, VectorTileLayer, MapView, SpatialReference, 
                    Point, Graphic, GraphicsLayer) {{
            try {{
                // URLs for the vector tiles
                var basemapVTURL = ""https://mapapi.geodata.gov.hk/gs/api/v1.0.0/vt/basemap/WGS84"";
                var mapLabelVTUrl = ""https://mapapi.geodata.gov.hk/gs/api/v1.0.0/vt/label/hk/{mapLanguage}/WGS84"";

                // Create the basemap
                var basemap = new Basemap({{
                    baseLayers: [
                        new VectorTileLayer({{
                            url: basemapVTURL,
                            copyright: '<a href=""https://api.portal.hkmapservice.gov.hk/disclaimer"" target=""_blank"" class=""copyright-url"">&copy; 地圖資料由地政總署提供</a><div class=""copyright-logo""></div>'
                        }})
                    ]
                }});
                
                // Create graphics layer for bus stops
                var graphicsLayer = new GraphicsLayer();
                
                // Create the map
                var map = new Map({{
                    basemap: basemap,
                    layers: [graphicsLayer]
                }});
                
                // Add the label layer
                map.add(new VectorTileLayer({{
                    url: mapLabelVTUrl
                }}));

                // Create the map view
                var mapView = new MapView({{
                    container: ""mapView"",
                    map: map,
                    zoom: 15,
                    center: [{centerLng.ToString("F6", CultureInfo.InvariantCulture)}, {centerLat.ToString("F6", CultureInfo.InvariantCulture)}],
                    constraints: {{
                        minZoom: 8,
                        maxZoom: 19
                    }}
                }});
                
                // Make mapView accessible globally for external navigation
                window.mapView = mapView;
                
                // Add bus stop markers
                try {{
                    {markersJs.ToString()}
                }} catch (e) {{
                    console.log('Error adding markers: ' + e.message);
                }}
                
                // If we have multiple stops, create a route line connecting them in sequence
                try {{
                    {(stops.Count > 1 ? GenerateRouteLine(stops) : "// No route line needed for single stop")}
                }} catch (e) {{
                    console.log('Error creating route line: ' + e.message);
                }}
            }} catch (e) {{
                console.log('Fatal error initializing map: ' + e.message);
            }}
        }});
    </script>
</head>
<body>
    <div id=""mapView""></div>
</body>
</html>";
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error generating map HTML");
            
            // Return a simple error HTML instead
            return $@"<!DOCTYPE html>
<html>
<head>
    <meta charset=""utf-8"">
    <title>Map Error</title>
    <style>
        body {{ font-family: sans-serif; padding: 20px; color: #444; text-align: center; }}
        .error {{ background-color: #ffeeee; border: 1px solid #ffaaaa; padding: 10px; border-radius: 5px; }}
    </style>
</head>
<body>
    <div class=""error"">
        <h3>Error Loading Map</h3>
        <p>There was an error generating the map. Please try again later.</p>
    </div>
</body>
</html>";
        }
    }

    private string GenerateRouteLine(List<TransportStop> stops)
    {
        try
        {
            if (stops.Count <= 1) return string.Empty;
            
            var routePathJs = new StringBuilder();
            
            // Create an array of path points
            routePathJs.AppendLine("var routePath = [");
            // Sort by index in the list which should already reflect sequence from database
            int index = 0;
            foreach (var stop in stops)
            {
                routePathJs.AppendLine($"    [{stop.Longitude.ToString("F6", CultureInfo.InvariantCulture)}, {stop.Latitude.ToString("F6", CultureInfo.InvariantCulture)}],");
                index++;
            }
            routePathJs.AppendLine("];");
            
            // Create and add the polyline - simplified to reduce syntax errors
            routePathJs.AppendLine(@"
            // Create the polyline
            var routePolyline = {
                type: ""polyline"",
                paths: [routePath]
            };
            
            // Create the line symbol
            var lineSymbol = {
                type: ""simple-line"",
                color: [50, 64, 240, 0.75],
                width: 4,
                style: ""solid""
            };
            
            // Create the graphic
            var routeGraphic = new Graphic({
                geometry: routePolyline,
                symbol: lineSymbol
            });
            
            // Add to the graphics layer
            graphicsLayer.add(routeGraphic);");
            
            return routePathJs.ToString();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error generating route line JavaScript");
            return "// Error generating route line";
        }
    }

    private void UpdateLabels()
    {
        try
        {
            FromLabel = App.GetString("From", "From: ");
            ToLabel = App.GetString("To", "To: ");
            
            if (Route != null)
            {
                // Force UI refresh after cleaning placeholders in Route property
                OnPropertyChanged(nameof(Route));
                OnPropertyChanged(nameof(LocalizedOrigin));
                OnPropertyChanged(nameof(LocalizedDestination));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error updating labels");
            FromLabel = "From: ";
            ToLabel = "To: ";
        }
    }

    public void SetRoute(TransportRoute route)
    {
        try
        {
            if (route == null)
            {
                _logger?.LogWarning("Attempted to set null route");
                return;
            }
            
            // Cancel any ongoing ETA refresh
            _etaRefreshCts?.Cancel();
            _etaData.Clear();
            
            // Clean placeholders from origin and destination
            CleanRoutePlaceholders(route);
            
            // Set the route which will trigger property changed notifications
            Route = route;
            
            // Set the title separately
            RouteTitle = $"{route.RouteNumber} {App.GetString("RouteDetails", "Route Details")}";
            _logger?.LogInformation("Setting route details for route {id}", route.Id);

            // Update labels with the cleaned data
            UpdateLabels();
            
            // Check if route has two directions (inbound/outbound)
            CheckRouteDirections(route);
            
            // Reset selected stop when setting a new route
            _selectedStop = null;
            
            // Load stops immediately and in background
            LoadStopsImmediately();
            if (!_isLoadingStops)
            {
                Task.Run(LoadStopsInBackground);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error setting route");
        }
    }
    
    protected override void OnAppearing()
    {
        base.OnAppearing();
        
        try
        {
            // Refresh ETAs when the page appears
            if (Route != null && Stops.Count > 0 && !_isLoadingEta)
            {
                _logger?.LogDebug("Page appearing - refreshing ETAs");
                Task.Run(async () => await FetchEtaForStops());
                
                // Also find nearest stop when page appears if not already finding it
                if (!_isLoadingStops)
                {
                    _ = FindAndScrollToNearestStop();
                }
            }
            
            // Start GPS refresh timer when page appears
            StartGpsRefreshTimer();
            
            // Start ETA display update timer
            StartEtaDisplayUpdateTimer();
            
            _ = RefreshFavoriteStatus();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in OnAppearing");
        }
    }
    
    private void LoadStopsImmediately()
    {
        try
        {
            _isLoadingStops = true;
            
            if (Route == null || string.IsNullOrEmpty(Route.Id))
            {
                _logger?.LogWarning("No route set for stop loading");
                return;
            }
            
            // Get stops for this route from the database
            var stops = _databaseService.GetSortedStopsForRoute(Route.Id);
            
            if (stops.Count > 0)
            {
                // Update the route's stops and UI
                Route.Stops = stops;
                Stops = new ObservableCollection<TransportStop>(stops);
                _logger?.LogDebug("Immediately loaded {count} stops for route {id}", stops.Count, Route.Id);
                
                // Force UI refresh
                OnPropertyChanged(nameof(Stops));
                
                // Automatically find the nearest stop
                MainThread.BeginInvokeOnMainThread(async () => await FindAndScrollToNearestStop());
            }
            else
            {
                _logger?.LogWarning("No stops found for route {id}", Route.Id);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error loading stops immediately");
        }
        finally
        {
            _isLoadingStops = false;
        }
    }
    
    private async Task LoadStopsInBackground()
    {
        try
        {
            _isLoadingStops = true;
            
            if (Route == null || string.IsNullOrEmpty(Route.Id))
                    {
                _logger?.LogWarning("No route set for background stop loading");
                        return;
                    }
                    
            // Simulate some work on a background thread 
            await Task.Run(() =>
                    {
                        try
                        {
                    var stops = _databaseService.GetSortedStopsForRoute(Route.Id);
                                
                    // ... existing code ...
                        }
                        catch (Exception ex)
                        {
                    _logger?.LogError(ex, "Error in background stop loading");
                        }
                    });
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error loading stops in background");
        }
        finally
        {
            _isLoadingStops = false;
        }
    }

    private async Task RefreshData()
    {
        try
        {
            IsRefreshing = true;
            
            // Load stops
            await LoadStopsInBackground();
            
            // Load ETAs
            await FetchEtaForStops();
            
            IsRefreshing = false;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error refreshing data");
            IsRefreshing = false;
        }
    }
    
    private async Task FetchEtaForStops()
    {
        if (_isLoadingEta || Route == null || Stops.Count == 0)
        {
            _logger?.LogWarning("Not fetching ETAs - isLoading={loading}, route={route}, stopCount={count}", 
                _isLoadingEta, Route != null, Stops.Count);
            return;
        }
        
        try
        {
            _isLoadingEta = true;
            _logger?.LogInformation("Starting to fetch ETAs for route {routeId} with {stopCount} stops", 
                Route.Id, Stops.Count);
                
            // Cancel any previous refresh task
            _etaRefreshCts?.Cancel();
            _etaRefreshCts?.Dispose();
            _etaRefreshCts = new CancellationTokenSource();
            var token = _etaRefreshCts.Token;
            
            // First, load existing ETAs from database
            await LoadEtasFromDatabase();
            
            if (token.IsCancellationRequested)
            {
                return;
            }
            
            // Then fetch ETAs from the service
            Dictionary<string, List<TransportEta>>? etaData = await FetchEtasFromApi(token);
            
            if (token.IsCancellationRequested || etaData == null)
            {
                return;
            }
            
            // Only update if we have new ETAs that are different
            bool hasNewEtas = HasNewEtas(etaData);
            _logger?.LogDebug("Has new ETAs? {hasNew}", hasNewEtas);
            
            if (hasNewEtas)
            {
                _etaData = etaData;
                
                // Update the UI with new ETA data on the main thread
                await MainThread.InvokeOnMainThreadAsync(() => UpdateStopsWithEtaData());
                
                // Save the new ETAs to database
                await SaveEtasToDatabase(etaData);
            }
            
            // Schedule the next refresh after 30 seconds
            ScheduleNextEtaRefresh(token);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error fetching ETAs");
        }
        finally
        {
            _isLoadingEta = false;
        }
    }
    
    private async Task<Dictionary<string, List<TransportEta>>?> FetchEtasFromApi(CancellationToken token)
    {
        if (Route == null) return null;
        
        Dictionary<string, List<TransportEta>>? etaData = null;
        try
        {
            _logger?.LogDebug("Fetching fresh ETAs from API for route {routeId}", Route.Id);
            
            // Check for MTR routes first
            bool isMtrRoute = Route.Id.StartsWith("MTR_");
            
            // Use the faster new bulk API for KMB routes
            bool isKmbRoute = !isMtrRoute && (Route.RouteNumber.StartsWith("KMB") || 
                              Route.RouteNumber.All(c => char.IsDigit(c) || c == 'X' || c == 'P' || c == 'A' || c == 'M' || c == 'N'));
            
            if (isMtrRoute)
            {
                _logger?.LogDebug("Using MTR API for route {routeNumber}", Route.RouteNumber);
                etaData = await _etaService.FetchEtaForRoute(
                    Route.Id, 
                    Route.RouteNumber, 
                    Route.ServiceType, 
                    Stops.ToList());
            }
            else if (isKmbRoute)
            {
                _logger?.LogDebug("Using fast bulk KMB API for route {routeNumber}", Route.RouteNumber);
                etaData = await _etaService.FetchKmbEtaForRoute(
                    Route.Id, 
                    Route.RouteNumber, 
                    Route.ServiceType);
                
                // Create dictionary using stop.Id as key in addition to StopId
                etaData = EnhanceEtaDataWithStopIds(etaData);
            }
            else
            {
                // Fallback to the generic method for other routes
                _logger?.LogDebug("Using generic API for route {routeNumber}", Route.RouteNumber);
                etaData = await _etaService.FetchEtaForRoute(
                    Route.Id, 
                    Route.RouteNumber, 
                    Route.ServiceType, 
                    Stops.ToList());
            }
            
            LogEtaData(etaData);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error fetching ETAs from service");
        }
        
        return etaData;
    }
    
    private Dictionary<string, List<TransportEta>>? EnhanceEtaDataWithStopIds(Dictionary<string, List<TransportEta>>? etaData)
    {
        if (etaData == null || Route == null) return etaData;
        
        var enhancedEtaData = new Dictionary<string, List<TransportEta>>(etaData);
        
        // Current route direction for filtering
        string routeDirection = Route.Bound;
        _logger?.LogDebug("Enhancing ETA data for route direction: {direction}", routeDirection);
        
        // Create a lookup from sequence number to TransportStop
        var stopsBySeq = new Dictionary<int, TransportStop>();
        foreach (var stop in Stops)
        {
            if (stop.Sequence > 0)
            {
                stopsBySeq[stop.Sequence] = stop;
            }
        }
        
        // Step 1: Process ETAs that have StopId
        // For each stop, if we have ETAs for its StopId, add them using stop.Id as key too
        foreach (var stop in Stops)
        {
            if (etaData.TryGetValue(stop.StopId, out var allEtas) && allEtas != null && allEtas.Count > 0)
            {
                // Filter ETAs to only include those matching the current route direction
                var filteredEtas = allEtas.Where(eta => 
                    eta != null && 
                    (eta.Direction?.Equals(routeDirection, StringComparison.OrdinalIgnoreCase) ?? false)).ToList();
                
                // If we have no ETAs with matching direction, check if we should use a fallback
                if (filteredEtas.Count == 0)
                {
                    _logger?.LogDebug("No ETAs found for direction {direction} for stop {stopId}, using fallback", 
                        routeDirection, stop.StopId);
                    
                    // Use all ETAs as fallback if direction matching fails
                    filteredEtas = allEtas.ToList();
                }
                
                if (filteredEtas.Count > 0)
                {
                    _logger?.LogDebug("Mapping {count} ETAs from StopId {stopId} to Id {id}", 
                        filteredEtas.Count, stop.StopId, stop.Id);
                    enhancedEtaData[stop.Id] = filteredEtas;
                }
            }
        }
        
        // Step 2: Process ETAs that only have sequence number but no StopId
        // Look for ETAs that are keyed by sequence number (as string) instead of stop ID
        foreach (var etaEntry in etaData)
        {
            // If the key could be parsed as an integer, it might be a sequence number
            if (int.TryParse(etaEntry.Key, out int seq) && stopsBySeq.TryGetValue(seq, out var stop))
            {
                var seqEtas = etaEntry.Value;
                if (seqEtas != null && seqEtas.Count > 0)
                {
                    // Filter ETAs to only include those matching the current route direction
                    var filteredSeqEtas = seqEtas.Where(eta => 
                        eta != null && 
                        (eta.Direction?.Equals(routeDirection, StringComparison.OrdinalIgnoreCase) ?? false)).ToList();
                    
                    // If we have no ETAs with matching direction, use a fallback
                    if (filteredSeqEtas.Count == 0)
                    {
                        filteredSeqEtas = seqEtas.ToList();
                    }
                    
                    if (filteredSeqEtas.Count > 0)
                    {
                        _logger?.LogDebug("Mapping {count} ETAs from sequence {seq} to stop ID {id}", 
                            filteredSeqEtas.Count, seq, stop.Id);
                        
                        // Update the StopId in each ETA to the correct value
                        foreach (var eta in filteredSeqEtas)
                        {
                            if (eta != null)
                            {
                                eta.StopId = stop.StopId;
                            }
                        }
                        
                        // Add by both stop.Id and stop.StopId to ensure we can find it in either case
                        enhancedEtaData[stop.Id] = filteredSeqEtas;
                        enhancedEtaData[stop.StopId] = filteredSeqEtas;
                    }
                }
            }
        }
        
        return enhancedEtaData;
    }
    
    private void LogEtaData(Dictionary<string, List<TransportEta>>? etaData)
    {
        _logger?.LogDebug("Received ETAs for {stopCount} stops", etaData?.Count ?? 0);
        
        if (etaData != null && etaData.Count > 0)
        {
            foreach (var stopEtas in etaData)
            {
                _logger?.LogInformation("Stop {stopId}: {count} ETAs", stopEtas.Key, stopEtas.Value.Count);
                foreach (var eta in stopEtas.Value.Take(3)) // Just log first few
                {
                    _logger?.LogInformation("  ETA: StopId={stopId}, Display={display}, Valid={valid}, Time={time}", 
                        eta.StopId, 
                        eta.DisplayEta, 
                        eta.IsValid, 
                        eta.EtaTime);
                }
            }
        }
        else
        {
            _logger?.LogWarning("No ETAs were returned from API");
        }
    }
    
    private void ScheduleNextEtaRefresh(CancellationToken token)
    {
        if (!token.IsCancellationRequested)
        {
            Dispatcher.StartTimer(TimeSpan.FromSeconds(30), () => 
            {
                if (_etaRefreshCts == null || _etaRefreshCts.IsCancellationRequested)
                {
                    return false; // Stop the timer
                }
                
                // Refresh ETAs without awaiting to prevent blocking
                _ = FetchEtaForStops();
                return false; // One-time timer
            });
        }
    }

    private bool HasNewEtas(Dictionary<string, List<TransportEta>> newEtaData)
    {
        if (newEtaData == null || newEtaData.Count == 0)
        {
            return false;
        }
        
        if (_etaData == null || _etaData.Count == 0)
        {
            return newEtaData.Count > 0;
        }
        
        // Check for new stops or different ETAs
        foreach (var stopId in newEtaData.Keys)
        {
            if (string.IsNullOrEmpty(stopId))
            {
                continue;
            }
            
            if (!_etaData.TryGetValue(stopId, out var existingEtas) || 
                existingEtas == null || 
                existingEtas.Count == 0)
            {
                return true; // New stop ETAs found
            }
            
            var newEtas = newEtaData[stopId];
            if (newEtas == null)
            {
                continue;
            }
            
            if (newEtas.Count != existingEtas.Count)
            {
                return true; // Different number of ETAs
            }
            
            // Compare each ETA
            for (int i = 0; i < newEtas.Count; i++)
            {
                if (i >= existingEtas.Count ||
                    newEtas[i] == null || existingEtas[i] == null ||
                    newEtas[i].DisplayEta != existingEtas[i].DisplayEta ||
                    newEtas[i].EtaTime != existingEtas[i].EtaTime)
                {
                    return true; // Different ETA values
                }
            }
        }
        
        // Check if any existing stops are missing in new data
        foreach (var stopId in _etaData.Keys)
        {
            if (!string.IsNullOrEmpty(stopId) && !newEtaData.ContainsKey(stopId))
            {
                return true; // Stop missing in new data
            }
        }
        
        return false; // No differences found
    }

    private void UpdateStopsWithEtaData()
    {
        try
        {
            if (Stops == null || _etaData == null)
            {
                _logger?.LogWarning("Cannot update stops with ETA data: Stops or ETA data is null");
                return;
            }
            
            int totalUpdated = 0;
            int emptyEtas = 0;
            
            _logger?.LogInformation("Updating {stopCount} stops with ETA data from {etaStopCount} entries", 
                Stops.Count, _etaData.Count);
            
            // For each stop, update the FirstEta property
            foreach (var stop in Stops)
            {
                if (stop == null || string.IsNullOrEmpty(stop.Id))
                {
                    continue;
                }
                
                string oldEta = stop.FirstEta; // Save old value for comparison
                string displayName = stop.LocalizedName ?? stop.NameEn ?? "[unnamed]";
                
                // Try to find ETAs for this stop using the Id first, then try StopId if no match
                if (_etaData.TryGetValue(stop.Id, out var etasById) && etasById != null && etasById.Count > 0)
                {
                    UpdateStopWithEta(stop, etasById, displayName, ref totalUpdated, oldEta);
                }
                else if (_etaData.TryGetValue(stop.StopId, out var etasByStopId) && etasByStopId != null && etasByStopId.Count > 0)
                {
                    UpdateStopWithEta(stop, etasByStopId, displayName, ref totalUpdated, oldEta);
                }
                else
                {
                    // No ETAs found for this stop
                    _logger?.LogDebug("No ETAs found for stop {stopId} ({name})", stop.Id, displayName);
                    stop.FirstEta = string.Empty;
                    emptyEtas++;
                }
            }
            
            _logger?.LogInformation("ETA Update Summary: {updated} stops updated, {empty} without ETAs", totalUpdated, emptyEtas);
            
            // Force collection refresh
            RefreshCollectionView();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error updating stops with ETA data");
        }
    }
    
    private void RefreshCollectionView()
    {
        // Force UI refresh for the entire collection
        OnPropertyChanged(nameof(Stops));
        
        // Also try to refresh the collection view directly if possible
        MainThread.InvokeOnMainThreadAsync(() => {
            try
            {
                if (this.FindByName<CollectionView>("StopsCollection") is CollectionView stopsCollection)
                {
                    // Force refresh of the collection view
                    var currentStops = Stops;
                    stopsCollection.ItemsSource = null;
                    stopsCollection.ItemsSource = currentStops;
                    _logger?.LogInformation("Explicitly refreshed StopsCollection with {count} stops", currentStops.Count);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error refreshing CollectionView");
            }
        });
    }

    // Helper method to update a stop with ETAs
    private void UpdateStopWithEta(TransportStop stop, List<TransportEta> etas, string displayName, ref int totalUpdated, string oldEta)
    {
        if (Route == null)
        {
            _logger?.LogWarning("Cannot update stop ETA, Route is null");
            return;
        }

        // Filter ETAs to only those matching this route's direction
        var filteredEtas = etas.Where(eta => 
            eta != null && 
            eta.IsValid && 
            (eta.Direction?.Equals(Route.Bound, StringComparison.OrdinalIgnoreCase) ?? false)).ToList();
        
        _logger?.LogDebug("Stop {stopId} ({name}): {filteredCount} matching ETAs for direction {direction} out of {totalCount}", 
            stop.Id, displayName, filteredEtas.Count, Route.Bound, etas.Count);
        
        if (filteredEtas.Count == 0)
        {
            _logger?.LogDebug("No matching ETAs found for route direction {direction} for stop {stopId} ({name})",
                Route.Bound, stop.Id, displayName);
            
            // Log all available directions to help diagnose the issue
            if (etas.Count > 0)
            {
                var availableDirections = etas
                    .Where(e => e != null)
                    .Select(e => e.Direction)
                    .Distinct()
                    .ToList();
                
                _logger?.LogDebug("Available directions for stop {stopId}: {directions}", 
                    stop.Id, string.Join(", ", availableDirections));
            }
            
            // Fallback to any valid ETA if we couldn't find any matching the direction
            // (this can happen if the API returns ETAs with slightly different direction codes)
            filteredEtas = etas.Where(eta => eta?.IsValid == true).ToList();
            
            if (filteredEtas.Count > 0)
            {
                _logger?.LogDebug("Using fallback ETAs for stop {stopId} ({name}) - direction mismatch", stop.Id, displayName);
            }
        }
        
        // Get the first VALID ETA for this stop
        var firstValidEta = filteredEtas.FirstOrDefault();
        if (firstValidEta != null)
        {
            string etaDisplay = firstValidEta.DisplayEta ?? string.Empty;
            stop.FirstEta = etaDisplay;
            
            _logger?.LogDebug("Updated stop {stopId} ({name}) with ETA: {eta} from direction {direction}", 
                stop.Id, displayName, etaDisplay, firstValidEta.Direction);
            
            if (oldEta != etaDisplay)
            {
                _logger?.LogDebug("  Changed ETA from '{oldEta}' to '{newEta}'", oldEta, etaDisplay);
                totalUpdated++;
            }
        }
        else
        {
            _logger?.LogDebug("No valid ETAs found among {count} ETAs for stop {stopId}", etas.Count, stop.Id);
            stop.FirstEta = string.Empty;
        }
    }
    
    private void LoadStops()
    {
        if (_isLoadingStops)
        {
            _logger?.LogDebug("Skipping LoadStops request as loading is already in progress");
            return;
        }
        
        try
        {
            // Set UI state
            IsRefreshing = true;
            
            // Call the background loading task
            Task.Run(async () => 
            {
                try
                {
                    await LoadStopsInBackground();
                    
                    // Load ETAs after stops are loaded
                    await FetchEtaForStops();
                    
                    // Ensure refreshing indicator is turned off
                    await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error in LoadStops task");
                    await MainThread.InvokeOnMainThreadAsync(() => IsRefreshing = false);
                }
            });
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error starting LoadStops task");
            IsRefreshing = false;
        }
    }

    // Handle language changes
    public void OnLanguageChanged()
    {
        try
        {
            UpdateLabels();
            
            // Update the route title
            if (Route != null)
            {
                RouteTitle = $"{Route.RouteNumber} {App.GetString("RouteDetails", "Route Details")}";
            }
            
            // Update the map to use the new language
            if (Stops.Count > 0)
            {
                UpdateMapWithStops();
            }
            else
            {
                // Just refresh the empty map with new language
                InitializeMap();
            }
            
            // Force refresh of stops to update localized names
            if (Stops.Count > 0)
            {
                var tempStops = new ObservableCollection<TransportStop>(Stops);
                Stops = new ObservableCollection<TransportStop>();
                Stops = tempStops;
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error handling language change");
        }
    }
    
    // Clean up resources
    protected override void OnDisappearing()
    {
        base.OnDisappearing();
        
        try
        {
            // Cancel any ongoing ETA refresh
            _etaRefreshCts?.Cancel();
            _etaRefreshCts?.Dispose();
            _etaRefreshCts = null;
            
            // Stop GPS refresh timer
            StopGpsRefreshTimer();
            
            // Stop ETA display update timer
            StopEtaDisplayUpdateTimer();
            
            // Clean up WebView event handlers
            StopsMapView.Navigated -= OnMapNavigated;
            StopsMapView.Navigating -= OnMapNavigating;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error cleaning up resources");
        }
    }

    private async Task SaveEtasToDatabase(Dictionary<string, List<TransportEta>> etaData)
    {
        try
        {
            // Flatten all ETAs into a single list
            var allEtas = etaData.Values.SelectMany(list => list).ToList();
            
            if (allEtas.Count > 0)
            {
                // Save to database in the background
            await _databaseService.SaveEtas(allEtas);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error saving ETAs to database");
        }
    }

    private void CleanRoutePlaceholders(TransportRoute route)
    {
        if (route == null)
        {
            return;
        }
        
        // Remove placeholders from origin fields
        if (route.OriginEn != null) route.OriginEn = route.OriginEn.Replace("{0}", "").Trim();
        if (route.OriginZh != null) route.OriginZh = route.OriginZh.Replace("{0}", "").Trim();
        if (route.OriginZhHant != null) route.OriginZhHant = route.OriginZhHant.Replace("{0}", "").Trim();
        if (route.OriginZhHans != null) route.OriginZhHans = route.OriginZhHans.Replace("{0}", "").Trim();
        
        // Remove placeholders from destination fields
        if (route.DestinationEn != null) route.DestinationEn = route.DestinationEn.Replace("{0}", "").Trim();
        if (route.DestinationZh != null) route.DestinationZh = route.DestinationZh.Replace("{0}", "").Trim();
        if (route.DestinationZhHant != null) route.DestinationZhHant = route.DestinationZhHant.Replace("{0}", "").Trim();
        if (route.DestinationZhHans != null) route.DestinationZhHans = route.DestinationZhHans.Replace("{0}", "").Trim();
    }

    private async Task LoadEtasFromDatabase()
    {
        try
        {
            if (Route == null || Stops == null || Stops.Count == 0)
            {
                return;
            }
            
            var stopIds = Stops.Where(s => s != null && !string.IsNullOrEmpty(s.Id))
                             .Select(s => s.Id).ToList();
            
            if (stopIds.Count == 0)
            {
                return;
            }
            
            Dictionary<string, List<TransportEta>> dbEtas = new Dictionary<string, List<TransportEta>>();
            
            // Rather than using GetDatabase which is not accessible, we'll directly get ETAs from the database service
            var etaData = await _etaService.GetEtasForRouteFromDb(Route.Id, stopIds);
            
            if (etaData != null && etaData.Count > 0)
            {
                // Update on main thread
                await MainThread.InvokeOnMainThreadAsync(() => 
                {
                    _etaData = etaData;
                    UpdateStopsWithEtaData();
                });
                _logger?.LogDebug("Loaded {count} ETAs from database", etaData.Sum(pair => pair.Value.Count));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error loading ETAs from database");
        }
    }

    /// <summary>
    /// Finds the nearest stop to the user's current location using geohash for better performance
    /// </summary>
    private async Task FindAndScrollToNearestStop()
    {
        try
        {
            if (Stops == null || Stops.Count == 0)
            {
                _logger?.LogDebug("No stops available to find nearest one");
                return;
            }

            // Get current location
            var location = await Geolocation.GetLocationAsync(new GeolocationRequest
            {
                DesiredAccuracy = GeolocationAccuracy.Best,
                Timeout = TimeSpan.FromSeconds(15)
            });

            if (location == null)
            {
                _logger?.LogWarning("Could not get current location");
                return;
            }

            _logger?.LogDebug("Current location: Lat {lat}, Lng {lng}", 
                location.Latitude, location.Longitude);

            // Generate geohash for current location at different precision levels
            string userGeoHash6 = GeoHash.Encode(location.Latitude, location.Longitude, 6);
            string userGeoHash7 = GeoHash.Encode(location.Latitude, location.Longitude, 7);
            string userGeoHash8 = GeoHash.Encode(location.Latitude, location.Longitude, 8);
            
            _logger?.LogDebug("User location geohash: GH6={gh6}, GH7={gh7}, GH8={gh8}", 
                userGeoHash6, userGeoHash7, userGeoHash8);
            
            // First filter: Get stops with matching geohash prefixes
            var potentialNearbyStops = new List<TransportStop>();
            
            // Strategy: First try precise match with GeoHash8 (smaller area)
            // If not enough matches, expand to GeoHash7, then GeoHash6
            foreach (var stop in Stops)
            {
                // For GeoHash8, match first 4 characters (very close proximity)
                if (!string.IsNullOrEmpty(stop.GeoHash8) && 
                    userGeoHash8.Length >= 4 && stop.GeoHash8.Length >= 4 &&
                    userGeoHash8.Substring(0, 4) == stop.GeoHash8.Substring(0, 4))
                {
                    potentialNearbyStops.Add(stop);
                }
            }
            
            // If not enough matches with GeoHash8, try with GeoHash7
            if (potentialNearbyStops.Count < 3)
            {
                foreach (var stop in Stops)
                {
                    // Skip if already added
                    if (potentialNearbyStops.Contains(stop))
                        continue;
                        
                    // For GeoHash7, match first 3 characters (close proximity)
                    if (!string.IsNullOrEmpty(stop.GeoHash7) && 
                        userGeoHash7.Length >= 3 && stop.GeoHash7.Length >= 3 &&
                        userGeoHash7.Substring(0, 3) == stop.GeoHash7.Substring(0, 3))
                    {
                        potentialNearbyStops.Add(stop);
                    }
                }
            }
            
            // If still not enough matches, try with GeoHash6
            if (potentialNearbyStops.Count < 5)
            {
                foreach (var stop in Stops)
                {
                    // Skip if already added
                    if (potentialNearbyStops.Contains(stop))
                        continue;
                        
                    // For GeoHash6, match first 2 characters (wider area)
                    if (!string.IsNullOrEmpty(stop.GeoHash6) && 
                        userGeoHash6.Length >= 2 && stop.GeoHash6.Length >= 2 &&
                        userGeoHash6.Substring(0, 2) == stop.GeoHash6.Substring(0, 2))
                    {
                        potentialNearbyStops.Add(stop);
                    }
                }
            }
            
            // If still no matches, use all stops as fallback
            if (potentialNearbyStops.Count == 0)
            {
                _logger?.LogDebug("No geohash matches found, using all stops");
                potentialNearbyStops = Stops.ToList();
            }
            else
            {
                _logger?.LogDebug("Filtered to {count} potential nearby stops using geohash", 
                    potentialNearbyStops.Count);
            }

            // Find the nearest stop within 1000m from the filtered set
            TransportStop? nearestStop = null;
            double nearestDistance = double.MaxValue;
            double maxDistance = 1000; // 1000 meters

            foreach (var stop in potentialNearbyStops)
            {
                if (stop == null) continue;
                
                // Create a location for the stop
                var stopLocation = new Location(stop.Latitude, stop.Longitude);
                
                // Calculate distance in meters
                double distanceInKm = Location.CalculateDistance(location, stopLocation, DistanceUnits.Kilometers);
                double distanceInMeters = distanceInKm * 1000;
                
                _logger?.LogDebug("Stop {stopName} (GH6={gh6}) is {distance}m away", 
                    stop.LocalizedName, stop.GeoHash6, distanceInMeters);
                
                if (distanceInMeters < nearestDistance && distanceInMeters <= maxDistance)
                {
                    nearestStop = stop;
                    nearestDistance = distanceInMeters;
                }
            }

            if (nearestStop != null)
            {
                _logger?.LogDebug("Nearest stop is {stopName} at {distance}m with GeoHash6={gh6}", 
                    nearestStop.LocalizedName, nearestDistance, nearestStop.GeoHash6);
                
                // Ensure UI updates happen on the main thread
                await MainThread.InvokeOnMainThreadAsync(async () =>
                {
                    // Scroll to the nearest stop in the collection view
                    StopsCollection.ScrollTo(nearestStop, position: ScrollToPosition.Start, animate: true);
                    
                    // Select the stop
                    StopsCollection.SelectedItem = nearestStop;
                    
                    // Also navigate the map to this stop
                    await NavigateMapToStop(nearestStop);
                });
            }
            else
            {
                _logger?.LogDebug("No stops found within {maxDistance}m", maxDistance);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error finding nearest stop");
        }
    }

    /// <summary>
    /// Initialize the GPS refresh timer that runs every 60 seconds
    /// </summary>
    private void InitializeGpsRefreshTimer()
    {
        _gpsRefreshTimer = Application.Current?.Dispatcher?.CreateTimer();
        if (_gpsRefreshTimer != null)
        {
            _gpsRefreshTimer.Interval = TimeSpan.FromSeconds(60);
            _gpsRefreshTimer.Tick += OnGpsRefreshTimerTick;
            _logger?.LogDebug("GPS refresh timer initialized");
        }
        else
        {
            _logger?.LogWarning("Could not create GPS refresh timer");
        }
    }
    
    /// <summary>
    /// Initialize the ETA display update timer that runs every 15 seconds
    /// </summary>
    private void InitializeEtaDisplayUpdateTimer()
    {
        _etaDisplayUpdateTimer = Application.Current?.Dispatcher?.CreateTimer();
        if (_etaDisplayUpdateTimer != null)
        {
            _etaDisplayUpdateTimer.Interval = TimeSpan.FromSeconds(15);
            _etaDisplayUpdateTimer.Tick += OnEtaDisplayUpdateTimerTick;
            _logger?.LogDebug("ETA display update timer initialized");
        }
        else
        {
            _logger?.LogWarning("Could not create ETA display update timer");
        }
    }
    
    /// <summary>
    /// Handle the GPS refresh timer tick event
    /// </summary>
    private void OnGpsRefreshTimerTick(object? sender, EventArgs e)
    {
        _logger?.LogDebug("GPS refresh timer tick");
        if (!_isGpsRefreshActive || Stops == null || Stops.Count == 0)
        {
            return;
        }
        
        // Refresh GPS position and find nearest stop
        _ = RefreshGpsPosition();
    }
    
    /// <summary>
    /// Refresh GPS position and find nearest stop
    /// </summary>
    private async Task RefreshGpsPosition()
    {
        if (_isGpsRefreshActive && !_isLoadingStops)
        {
            // Cancel any previous GPS refresh operation
            _gpsRefreshCts?.Cancel();
            _gpsRefreshCts?.Dispose();
            _gpsRefreshCts = new CancellationTokenSource();
            
            try
            {
                _logger?.LogDebug("Refreshing GPS position");
                await FindAndScrollToNearestStop();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error refreshing GPS position");
            }
        }
    }
    
    /// <summary>
    /// Start GPS refresh timer
    /// </summary>
    private void StartGpsRefreshTimer()
    {
        if (_gpsRefreshTimer != null && !_isGpsRefreshActive)
        {
            _isGpsRefreshActive = true;
            _gpsRefreshTimer.Start();
            _logger?.LogDebug("GPS refresh timer started");
        }
    }
    
    /// <summary>
    /// Stop GPS refresh timer
    /// </summary>
    private void StopGpsRefreshTimer()
    {
        if (_gpsRefreshTimer != null && _isGpsRefreshActive)
        {
            _gpsRefreshTimer.Stop();
            _isGpsRefreshActive = false;
            _logger?.LogDebug("GPS refresh timer stopped");
            
            // Cancel any ongoing GPS refresh operation
            _gpsRefreshCts?.Cancel();
        }
    }
    
    /// <summary>
    /// Start ETA display update timer
    /// </summary>
    private void StartEtaDisplayUpdateTimer()
    {
        if (_etaDisplayUpdateTimer != null && !_etaDisplayUpdateTimer.IsRunning)
        {
            _etaDisplayUpdateTimer.Start();
            _logger?.LogDebug("ETA display update timer started");
        }
    }
    
    /// <summary>
    /// Stop ETA display update timer
    /// </summary>
    private void StopEtaDisplayUpdateTimer()
    {
        if (_etaDisplayUpdateTimer != null && _etaDisplayUpdateTimer.IsRunning)
        {
            _etaDisplayUpdateTimer.Stop();
            _logger?.LogDebug("ETA display update timer stopped");
        }
    }
    
    /// <summary>
    /// Handle the ETA display update timer tick event
    /// </summary>
    private void OnEtaDisplayUpdateTimerTick(object? sender, EventArgs e)
    {
        try
        {
            // Skip if ETA update is already in progress
            if (_isLoadingEta || _isUpdatingEtaDisplay)
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
            // Only process if we have stops loaded
            if (Stops == null || Stops.Count == 0 || _etaData.Count == 0)
            {
                return;
            }
            
            bool anyUpdates = false;
            
            // Current time to compare against
            DateTime now = DateTime.Now;
            
            // Process each stop with updated ETA display
            foreach (var stop in Stops)
            {
                if (stop == null || !_etaData.ContainsKey(stop.Id)) continue;
                
                var stopEtas = _etaData[stop.Id];
                if (stopEtas == null || !stopEtas.Any()) continue;
                
                // Get the earliest ETA
                var earliestEta = stopEtas.OrderBy(e => e.EtaTime).FirstOrDefault();
                if (earliestEta == null || earliestEta.EtaTime == DateTime.MinValue) continue;
                
                // Calculate new display text based on current time
                string oldDisplayText = stop.FirstEta;
                
                // Update the display text based on current time
                TimeSpan timeDiff = earliestEta.EtaTime - now;
                
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
                    stop.FirstEta = newDisplayText;
                    anyUpdates = true;
                }
            }
            
            // If any updates were made, log it
            if (anyUpdates)
            {
                _logger?.LogDebug("Updated ETA display text for route stops without fetching new data");
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error updating ETA display text");
        }
    }

    private void CheckRouteDirections(TransportRoute route)
    {
        if (route == null) return;
        
        try
        {
            int directionCount = _databaseService.CountRouteDirections(
                route.RouteNumber, 
                route.ServiceType, 
                route.Operator);
            
            _hasTwoDirections = directionCount > 1;
            
            if (_hasTwoDirections)
            {
                _logger?.LogDebug("Route {route} has multiple directions", route.RouteNumber);
                var directions = _databaseService.GetRouteDirections(
                    route.RouteNumber, 
                    route.ServiceType, 
                    route.Operator);
                
                _logger?.LogDebug("Available directions for route {route}: {directions}", 
                    route.RouteNumber, string.Join(", ", directions));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error checking route directions");
            _hasTwoDirections = false;
        }
    }
    
    private void ReverseRouteDirection()
    {
        if (Route == null) return;
        
        try
            {
            _logger?.LogInformation("Reversing route direction for {route}", Route.RouteNumber);
            
            // Get all possible directions for this route
            var allDirections = _databaseService.GetRouteDirections(
                Route.RouteNumber, 
                Route.ServiceType, 
                Route.Operator);
            
            // Remove the current direction from the list
            var filteredDirections = allDirections.Where(d => d != Route.Bound).ToList();
            
            if (filteredDirections.Count == 0)
            {
                _logger?.LogWarning("No alternative directions found for route {route}", Route.RouteNumber);
                return;
            }
            
            // Get the next direction (usually just one, but we'll handle multiple just in case)
            string nextDirection = filteredDirections.First();
            
            // Get the opposite direction route
            var oppositeRoute = _databaseService.GetRouteByNumberBoundAndServiceType(
                Route.RouteNumber,
                nextDirection,
                Route.ServiceType,
                Route.Operator);
            
            if (oppositeRoute != null)
            {
                // Cancel any ongoing ETA refresh
                _etaRefreshCts?.Cancel();
                
                // Set the new route
                SetRoute(oppositeRoute);
                
                // Fetch new ETAs
                _ = FetchEtaForStops();
            }
            else
            {
                _logger?.LogWarning("Could not find opposite route for {routeId}", Route.Id);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error reversing route direction");
        }
    }

    private async Task RefreshFavoriteStatus()
    {
        if (Route == null) return;
        var favorites = _databaseService.GetAllRecords<FavoriteRoute>("FavoriteRoutes");
        _isFavorite = favorites.Any(f => f.RouteId == Route.Id);
        if (_isFavorite)
        {
            FavoriteIconGlyph = "\uf005"; // Star filled
            FavoriteIconFontFamily = "FontAwesomeSolid";
        }
        else
        {
            FavoriteIconGlyph = "\uf006"; // Star outline
            FavoriteIconFontFamily = "FontAwesomeRegular";
        }
    }

    private async Task ToggleFavorite()
    {
        if (Route == null) return;
        var favorites = _databaseService.GetAllRecords<FavoriteRoute>("FavoriteRoutes");
        var fav = favorites.FirstOrDefault(f => f.RouteId == Route.Id);
        if (fav != null)
        {
            _databaseService.DeleteRecord<FavoriteRoute>("FavoriteRoutes", fav.Id);
            _isFavorite = false;
        }
        else
        {
            var newFav = new FavoriteRoute { RouteId = Route.Id };
            _databaseService.InsertRecord("FavoriteRoutes", newFav);
            _isFavorite = true;
        }
        if (_isFavorite)
        {
            FavoriteIconGlyph = "\uf005"; // Star filled
            FavoriteIconFontFamily = "FontAwesomeSolid";
        }
        else
        {
            FavoriteIconGlyph = "\uf006"; // Star outline
            FavoriteIconFontFamily = "FontAwesomeRegular";
        }
    }
} 