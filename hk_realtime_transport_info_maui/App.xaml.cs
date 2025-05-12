using hk_realtime_transport_info_maui.Services;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Globalization;
using hk_realtime_transport_info_maui.Resources;
using Microsoft.Maui.Controls.Xaml;
using System.Reflection;
using System.Linq;

namespace hk_realtime_transport_info_maui;

public partial class App : Application
{
	private readonly KmbDataService _kmbDataService;
	private readonly ILogger<App> _logger;
	private readonly DatabaseService _databaseService;
	private static readonly ConcurrentDictionary<string, Action> _dataReadyCallbacks = new();
	private bool _dataInitialized;
	private AppBackgroundService? _backgroundService;
	
	// Add property to suppress auto-refresh
	public bool SuppressAutoDataRefresh { get; set; }
	
	// Constants
	private const int DATA_INITIALIZATION_TIMEOUT_MS = 60000; // 60 second timeout
	
	// Dictionary to store current string resources
	public static Dictionary<string, string> StringResources { get; private set; } = new Dictionary<string, string>
	{
		{ "AppTitle", "Route Information" },
		{ "AllRoutes", "All Routes" },
		{ "From", "From: " },
		{ "To", "To: " },
		{ "RouteDetails", "Route Details" },
		{ "Route", "Route: {0}" },
		{ "Name", "Name: {0}" },
		{ "Close", "Close" },
		{ "NoRoutesFound", "No routes found." },
		{ "Refresh", "Refresh" }
	};

	public App(KmbDataService kmbDataService, ILogger<App> logger, DatabaseService databaseService)
	{
		// Initialize components
		InitializeComponent();
		
		_kmbDataService = kmbDataService;
		_logger = logger;
		_databaseService = databaseService;
		
		// Set up UI exception handling
		SetupExceptionHandling();
		
		// Initialize background service
		var services = IPlatformApplication.Current?.Services;
		_backgroundService = services?.GetService<AppBackgroundService>();
		
		// Initialize data in background with a delay to let UI render first
		MainThread.BeginInvokeOnMainThread(() => 
		{
		    // Start data initialization in background
		    _ = Task.Run(() => InitializeDataAsync());
		});
	}
	
	// Set up platform-specific exception handling
	private void SetupExceptionHandling()
	{
		// Use the exception handling service when available
		var exceptionHandler = IPlatformApplication.Current?.Services?.GetService<ExceptionHandlingService>();
		
		// Handle exceptions in MAUI
		AppDomain.CurrentDomain.FirstChanceException += (sender, args) =>
		{
			// Log first chance exceptions but don't handle them yet
			_logger.LogDebug(args.Exception, "First chance exception: {Message}", args.Exception.Message);
		};
		
		// Use platform-specific exception handling
#if ANDROID
        // Android-specific exception handling
        Android.Runtime.AndroidEnvironment.UnhandledExceptionRaiser += (sender, args) =>
        {
			if (exceptionHandler != null)
			{
				// Use the central exception handler
				bool handled = exceptionHandler.HandleUnhandledException(args.Exception);
				args.Handled = handled;
			}
			else
			{
				// Fallback to basic handling
				args.Handled = true;
				_logger.LogError(args.Exception, "Android unhandled exception: {Message}", args.Exception.Message);
				ShowErrorAlert("An unexpected error occurred. The application will continue running but some features may be affected.");
			}
        };
#elif IOS
        // iOS-specific exception handling
        ObjCRuntime.Runtime.MarshalManagedException += (sender, args) =>
        {
			if (exceptionHandler != null)
			{
				// Use the central exception handler
				exceptionHandler.HandleUnhandledException(args.Exception);
			}
			else
			{
				// Fallback to basic handling
				_logger.LogError(args.Exception, "iOS unhandled exception: {Message}", args.Exception.Message);
				ShowErrorAlert("An unexpected error occurred. The application will continue running but some features may be affected.");
			}
			
			// Prevent app termination by unwinding the native code
            args.ExceptionMode = ObjCRuntime.MarshalManagedExceptionMode.UnwindNativeCode;
        };
#endif

		// Register a global handler for uncaught exceptions on the UI thread
		// Using a try-catch pattern instead of dispatcher events which may not be available
		try
		{
			// Add a global handler for UI operations
			if (Current != null && Current.Dispatcher != null)
			{
				_logger.LogInformation("Setting up UI exception handling");
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to set up UI exception handling");
		}
	}
	
	// Helper method to display error alerts on the main thread
	private void ShowErrorAlert(string message)
	{
		if (MainThread.IsMainThread)
		{
			DisplayErrorAlert();
		}
		else
		{
			MainThread.BeginInvokeOnMainThread(DisplayErrorAlert);
		}
		
		void DisplayErrorAlert()
		{
			try
			{
				if (Current != null)
				{
					var window = Current.Windows.FirstOrDefault();
					if (window?.Page != null)
					{
						window.Page.DisplayAlert("Application Error", message, "OK");
					}
				}
			}
			catch (Exception ex)
			{
				// Log but don't crash if alert display fails
				_logger.LogError(ex, "Failed to display error alert");
			}
		}
	}

	protected override void OnStart()
	{
		base.OnStart();
		_logger.LogInformation("Application started");
		
		// Start the background service
		_backgroundService?.Start();
	}
	
	protected override void OnSleep()
	{
		base.OnSleep();
		_logger.LogInformation("Application going to sleep");
		
		// Stop background service to save resources while app is sleeping
		_backgroundService?.Stop();
	}
	
	protected override void OnResume()
	{
		base.OnResume();
		_logger.LogInformation("Application resuming");
		
		// Restart background service
		_backgroundService?.Start();
		
		// Check if we need to re-initialize data
		if (!_dataInitialized)
		{
		    _ = Task.Run(() => InitializeDataAsync());
		}
	}
	
	protected override void CleanUp()
	{
		base.CleanUp();
		
		// Stop background service and dispose database connection
		_logger.LogInformation("Application shutting down, disposing resources");
		_backgroundService?.Stop();
		_databaseService.Dispose();
	}

	protected override Window CreateWindow(IActivationState? activationState)
	{
		var services = IPlatformApplication.Current?.Services;
		if (services == null)
		{
			_logger.LogError("Failed to get services from IPlatformApplication.Current");
			throw new InvalidOperationException("IPlatformApplication.Current.Services is null");
		}
		
		var logger = services.GetService<ILogger<MainPage>>();
		if (logger == null)
		{
			_logger.LogWarning("ILogger<MainPage> not found in service provider, creating default logger");
			var loggerFactory = services.GetService<ILoggerFactory>();
			logger = loggerFactory?.CreateLogger<MainPage>();
		}
		
		if (logger == null)
		{
			_logger.LogError("Failed to create logger for MainPage");
			throw new InvalidOperationException("Failed to create logger for MainPage");
		}
		
		var etaService = services.GetService<EtaService>();
		if (etaService == null)
		{
			_logger.LogError("Failed to get EtaService from service provider");
			throw new InvalidOperationException("EtaService not found in service provider");
		}
		
		return new Window(new AppShell(_databaseService, _kmbDataService, etaService, logger));
	}
	
	private async Task InitializeDataAsync(bool forceRefresh = false)
	{
		try
		{
			// Start data download process (returns immediately due to improvements)
			await _kmbDataService.DownloadAllDataAsync(forceRefresh).ConfigureAwait(false);
			
			// Register for progress events
			EventHandler<DownloadProgressEventArgs>? progressHandler = null;
			var completionSource = new TaskCompletionSource<bool>();
			var dataWasUpdated = false;
			
			progressHandler = (sender, e) => 
			{
				if (e.Progress >= 1.0 || e.StatusMessage == "DOWNLOAD_COMPLETE")
				{
					// Data download is complete
					dataWasUpdated = e.DataWasUpdated;
					
					// Optimize the database if data was updated
					if (dataWasUpdated)
					{
						// Run database optimization on a background thread
						Task.Run(() => {
							try {
								_logger.LogInformation("Running database optimization after data update");
								_databaseService.AnalyzeAndOptimizeDatabase();
							}
							catch (Exception ex) {
								_logger.LogError(ex, "Error optimizing database after data update");
							}
						});
					}
					
					completionSource.TrySetResult(true);
				}
			};
			
			// Add the handler
			_kmbDataService.ProgressChanged += progressHandler;
			
			try 
			{
				// Wait for completion with timeout
				await Task.WhenAny(completionSource.Task, Task.Delay(DATA_INITIALIZATION_TIMEOUT_MS));
			} 
			finally 
			{
				// Always remove the handler
				_kmbDataService.ProgressChanged -= progressHandler;
			}
			
			_dataInitialized = true;
			_logger.LogInformation("Transport data initialization completed");
			
			// Only trigger callbacks if data actually changed
			if (dataWasUpdated)
			{
				// Notify all registered callbacks that data is ready on the main thread
				_logger.LogInformation("Triggering {count} data ready callbacks", _dataReadyCallbacks.Count);
				
				// Ensure callbacks are executed on main thread
				await MainThread.InvokeOnMainThreadAsync(() => 
				{
					foreach (var callback in _dataReadyCallbacks.Values)
					{
						try
						{
							callback();
						}
						catch (Exception ex)
						{
							_logger.LogError(ex, "Error executing data ready callback");
						}
					}
				});
			}
			else
			{
				_logger.LogInformation("Skipping callbacks as no data was updated");
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error initializing transport data");
		}
	}

	// Modify the ForceRefreshData method to improve logging and suppression check
	public void ForceRefreshData()
	{
		// Add detailed logging to help identify what's triggering refreshes
		_logger?.LogInformation("ForceRefreshData called. Stack trace:");
		string stackTrace = "";
		try
		{
			throw new Exception("Stack trace capture for refresh trigger identification");
		}
		catch (Exception ex)
		{
			stackTrace = ex.StackTrace ?? "";
			_logger?.LogInformation(stackTrace);
		}
		
		// Check if this is being called during distance filter operations
		if (stackTrace.Contains("OnDistanceFilterClicked") || 
		    stackTrace.Contains("ApplyDistanceFilterAsync") || 
		    stackTrace.Contains("IncrementallyUpdateStopGroups"))
		{
			_logger?.LogInformation("Auto data refresh prevented during distance filter change operation");
			return;
		}
		
		// Check if auto-refresh is temporarily suppressed
		if (SuppressAutoDataRefresh)
		{
			_logger?.LogWarning("Auto data refresh suppressed by SuppressAutoDataRefresh flag, skipping refresh");
			return;
		}
		
		// Only proceed with the refresh if suppression is not active
		_logger?.LogInformation("ForceRefreshData proceeding with data refresh");
		Task.Run(() => InitializeDataAsync(true));
	}

	public static void RegisterDataReadyCallback(string key, Action callback)
	{
		_dataReadyCallbacks.TryAdd(key, callback);
	}

	public static void UnregisterDataReadyCallback(string key)
	{
		_dataReadyCallbacks.TryRemove(key, out _);
	}
	
	// Method to change culture dynamically
	public static void ChangeCulture(string cultureName)
	{
		try
		{
			var cultureInfo = new CultureInfo(cultureName);
			CultureInfo.DefaultThreadCurrentCulture = cultureInfo;
			CultureInfo.DefaultThreadCurrentUICulture = cultureInfo;
			
			// Update string resources based on culture
			UpdateStringResources(cultureName);
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error changing culture: {ex.Message}");
		}
	}
	
	// Method to update string resources based on culture
	private static void UpdateStringResources(string cultureName)
	{
		// Default to English
		StringResources["AppTitle"] = "Route Information";
		StringResources["AllRoutes"] = "All Routes";
		StringResources["From"] = "From: ";
		StringResources["To"] = "To: ";
		StringResources["RouteDetails"] = "Route Details";
		StringResources["Route"] = "Route: {0}";
		StringResources["Name"] = "Name: {0}";
		StringResources["Close"] = "Close";
		StringResources["NoRoutesFound"] = "No routes found.";
		StringResources["Refresh"] = "Refresh";
		
		// Update based on culture
		if (cultureName.StartsWith("zh-Hant") || cultureName == "zh-HK" || cultureName == "zh-TW")
		{
			// Traditional Chinese
			StringResources["AppTitle"] = "路線資訊";
			StringResources["AllRoutes"] = "所有路線";
			StringResources["From"] = "從: ";
			StringResources["To"] = "到: ";
			StringResources["RouteDetails"] = "路線詳情";
			StringResources["Route"] = "路線: {0}";
			StringResources["Name"] = "名稱: {0}";
			StringResources["Close"] = "關閉";
			StringResources["NoRoutesFound"] = "未找到路線。";
			StringResources["Refresh"] = "刷新";
		}
		else if (cultureName.StartsWith("zh"))
		{
			// Simplified Chinese
			StringResources["AppTitle"] = "路线信息";
			StringResources["AllRoutes"] = "所有路线";
			StringResources["From"] = "从: ";
			StringResources["To"] = "到: ";
			StringResources["RouteDetails"] = "路线详情";
			StringResources["Route"] = "路线: {0}";
			StringResources["Name"] = "名称: {0}";
			StringResources["Close"] = "关闭";
			StringResources["NoRoutesFound"] = "未找到路线。";
			StringResources["Refresh"] = "刷新";
		}
	}
	
	// Get a string resource by key
	public static string GetString(string key, string defaultValue = "")
	{
		if (StringResources.TryGetValue(key, out var value))
		{
			return value;
		}
		return defaultValue;
	}
}