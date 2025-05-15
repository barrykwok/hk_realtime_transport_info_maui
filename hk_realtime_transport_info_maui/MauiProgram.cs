using Microsoft.Extensions.Logging;
using hk_realtime_transport_info_maui.Services;
using System.Globalization;
using Microsoft.Extensions.Caching.Memory;

namespace hk_realtime_transport_info_maui;

public static class MauiProgram
{
	// Global exception handler reference for use in handlers
	private static ExceptionHandlingService? _exceptionHandler;

	public static MauiApp CreateMauiApp()
	{
		// Register global exception handlers
		AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
		TaskScheduler.UnobservedTaskException += OnUnobservedTaskException;
		
		// Memory optimization removed - not supported on iOS
		// GC.TryStartNoGCRegion(100 * 1024 * 1024); // Reserve 100MB to prevent GC during app startup
		
		// Set the culture based on device settings
		var culture = CultureInfo.CurrentCulture;
		CultureInfo.DefaultThreadCurrentCulture = culture;
		CultureInfo.DefaultThreadCurrentUICulture = culture;

		var builder = MauiApp.CreateBuilder();
		builder
			.UseMauiApp<App>()
			.ConfigureFonts(fonts =>
			{
				fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
				fonts.AddFont("OpenSans-Semibold.ttf", "OpenSansSemibold");
				fonts.AddFont("fa-solid-900.ttf", "FontAwesomeSolid");
			});

		// Configure logging
		builder.Logging.ClearProviders(); // Remove default providers
		builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace); 
			
#if DEBUG
		builder.Logging
			.AddCustomLogger(config => 
			{
				config.MinimumLogLevel = Microsoft.Extensions.Logging.LogLevel.Debug;
				config.IncludeExceptions = true;
			});
#else
		builder.Logging
			.AddCustomLogger(config => 
			{
				config.MinimumLogLevel = Microsoft.Extensions.Logging.LogLevel.Information;
				config.IncludeExceptions = true;
			});
#endif

		// Register cache services
		builder.Services.AddMemoryCache(options => 
		{
			// Configure memory cache with size limit
			options.SizeLimit = 1000; // Units of size rather than bytes (more manageable)
			options.CompactionPercentage = 0.2; // Remove 20% when limit is reached
			options.ExpirationScanFrequency = TimeSpan.FromMinutes(1);
		});

		// Configure cache options
		builder.Services.Configure<CacheOptions>(options => 
		{
			options.DefaultExpiration = TimeSpan.FromMinutes(5);
			options.DefaultSlidingExpiration = TimeSpan.FromMinutes(2);
			options.SizeLimit = 1000;
			options.EnableCacheEvents = true;
			options.ExpirationScanFrequency = TimeSpan.FromMinutes(1);
			options.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1);
		});
		
		// Register the enhanced cache service
		builder.Services.AddSingleton<CacheService>();
		
		// Register performance optimizer service
		builder.Services.AddSingleton<PerformanceOptimizer>();
		
		// Register exception handling service
		builder.Services.AddSingleton<ExceptionHandlingService>();

		// Register LiteDbService
		builder.Services.AddSingleton<LiteDbService>(sp => 
			new LiteDbService(sp.GetService<ILogger<LiteDbService>>()));
		
		// Register transport data services
		builder.Services.AddSingleton<KmbDataService>(sp =>
		{
			var httpClientUtility = sp.GetRequiredService<HttpClientUtility>();
			var liteDbService = sp.GetRequiredService<LiteDbService>();
			var logger = sp.GetRequiredService<ILogger<KmbDataService>>();
			
			return new KmbDataService(liteDbService, httpClientUtility, logger);
		});
		
		// Register MTR data service
		builder.Services.AddSingleton<MtrDataService>(sp =>
		{
			var httpClientUtility = sp.GetRequiredService<HttpClientUtility>();
			var liteDbService = sp.GetRequiredService<LiteDbService>();
			var logger = sp.GetRequiredService<ILogger<MtrDataService>>();
			var cacheService = sp.GetRequiredService<CacheService>();
			
			return new MtrDataService(liteDbService, httpClientUtility, logger, cacheService);
		});
		
		// Register ETA service
		builder.Services.AddSingleton<EtaService>(sp => 
		{
			var httpClient = sp.GetRequiredService<HttpClient>();
			var liteDbService = sp.GetRequiredService<LiteDbService>();
			var logger = sp.GetRequiredService<ILogger<EtaService>>();
			var cacheService = sp.GetRequiredService<CacheService>();
			
			return new EtaService(httpClient, liteDbService, logger, cacheService);
		});
		
		// Register location cache service
		builder.Services.AddSingleton<LocationCacheService>(sp =>
		{
			var liteDbService = sp.GetRequiredService<LiteDbService>();
			var logger = sp.GetRequiredService<ILogger<LocationCacheService>>();
			
			return new LocationCacheService(liteDbService, logger);
		});
		
		// Register HttpClient for API services
		builder.Services.AddSingleton<HttpClient>();
		
		// Register HTTP utilities
		builder.Services.AddSingleton<HttpClientUtility>(sp => 
		{
			var logger = sp.GetService<ILogger<HttpClientUtility>>();
			var memoryCache = sp.GetService<IMemoryCache>();
			var options = sp.GetService<HttpClientUtilityOptions>();
			var exceptionHandler = sp.GetService<ExceptionHandlingService>();
			
			// Ensure none of these are null
			if (logger == null)
			{
				var loggerFactory = sp.GetService<ILoggerFactory>();
				logger = loggerFactory?.CreateLogger<HttpClientUtility>();
			}
			
			// Create a memory cache if it's null
			memoryCache ??= new MemoryCache(new MemoryCacheOptions());
			
			return new HttpClientUtility(logger!, memoryCache, options, exceptionHandler);
		});
		builder.Services.AddSingleton<HttpClientUtilityOptions>(sp => new HttpClientUtilityOptions
		{
			MaxConcurrentRequests = 4,
			MaxRetryAttempts = 3,
			InitialRetryDelayMs = 1000,
			MaxRetryDelayMs = 15000,
			TimeoutSeconds = 30,
			AppVersion = "1.0.0",
			EnableResponseCaching = true,
			EnableHttp2 = true,
			EnableCompression = true,
			CacheEntrySize = 1 // Each entry takes up 1 unit of cache size
		});

		// Register pages
		builder.Services.AddTransient<MainPage>(sp => {
			var databaseService = sp.GetRequiredService<LiteDbService>();
			var kmbDataService = sp.GetRequiredService<KmbDataService>();
			var mtrDataService = sp.GetRequiredService<MtrDataService>();
			var etaService = sp.GetRequiredService<EtaService>();
			var logger = sp.GetRequiredService<ILogger<MainPage>>();
			var locationCacheService = sp.GetRequiredService<LocationCacheService>();
			return new MainPage(databaseService, kmbDataService, mtrDataService, etaService, logger, locationCacheService);
		});
		builder.Services.AddTransient<RouteDetailsPage>();

		// Register background service for database maintenance
		builder.Services.AddSingleton<AppBackgroundService>();

		var app = builder.Build();
		
		// Store a reference to the exception handler
		_exceptionHandler = app.Services.GetService<ExceptionHandlingService>();
		
		return app;
	}
	
	// Global handler for unhandled exceptions
	private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
	{
		var exception = e.ExceptionObject as Exception;
		var isTerminating = e.IsTerminating;
		
		try
		{
			// Use the exception handling service if available
			if (_exceptionHandler != null && exception != null)
			{
				_exceptionHandler.HandleUnhandledException(exception, showUI: true);
			}
			else
			{
				// Fallback to basic logging
				var logger = GetLogger();
				logger?.LogCritical(exception, "Unhandled exception caught: {Message}, IsTerminating: {IsTerminating}", 
					exception?.Message ?? "Unknown error", isTerminating);
				
				// Display error to user
				MainThread.BeginInvokeOnMainThread(() =>
				{
                    try
                    {
                        if (Application.Current != null)
                        {
                            var window = Application.Current.Windows.FirstOrDefault();
                            if (window?.Page != null)
                            {
                                window.Page.DisplayAlert(
                                    "Application Error", 
                                    "An unexpected error occurred. The app will try to recover automatically.", 
                                    "OK");
                            }
                        }
                    }
                    catch
                    {
                        // If we can't show UI alert, at least log to console
                        Console.WriteLine($"Critical error: {exception?.Message ?? "Unknown error"}");
                    }
				});
			}
			
			// If the app is terminating, try to perform cleanup
			if (isTerminating)
			{
				try
				{
					// Attempt to perform cleanup
					CleanupResources();
				}
				catch
				{
					// Ignore cleanup errors at this point
				}
			}
		}
		catch
		{
			// Last resort exception handling - don't let this handler itself crash
			try
			{
				Console.WriteLine($"Critical error in exception handler: {exception?.Message ?? "Unknown error"}");
			}
			catch
			{
				// Give up if we can't even write to console
			}
		}
	}
	
	// Handler for unobserved task exceptions
	private static void OnUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
	{
		// Mark the exception as observed to prevent the process from being terminated
		e.SetObserved();
		
		var exception = e.Exception;
		
		try
		{
			// Use the exception handling service if available
			if (_exceptionHandler != null && exception != null)
			{
				_exceptionHandler.HandleUnhandledException(exception, showUI: false);
			}
			else
			{
				// Fallback to basic logging
				var logger = GetLogger();
				logger?.LogError(exception, "Unobserved task exception caught: {Message}", 
					exception?.Message ?? "Unknown error");
			}
		}
		catch
		{
			// Last resort - don't let the handler crash
			try
			{
				Console.WriteLine($"Error in task exception handler: {exception?.Message ?? "Unknown error"}");
			}
			catch
			{
				// Give up if we can't even write to console
			}
		}
	}
	
	// Helper method to get logger from the application
	private static ILogger? GetLogger()
	{
		try
		{
			if (Application.Current is App app)
			{
				var services = IPlatformApplication.Current?.Services;
				if (services != null)
				{
					// Get a non-generic logger from the logger factory
					var loggerFactory = services.GetService<ILoggerFactory>();
					return loggerFactory?.CreateLogger("GlobalExceptionHandler");
				}
			}
		}
		catch
		{
			// Ignore errors when trying to get logger
		}
		
		return null;
	}
	
	// Helper method to clean up resources when the app is terminating
	private static void CleanupResources()
	{
		try
		{
			if (Application.Current is App app)
			{
				var services = IPlatformApplication.Current?.Services;
				if (services != null)
				{
					// Get the database service and dispose it
					var liteDbService = services.GetService<LiteDbService>();
					liteDbService?.Dispose();
				}
			}
		}
		catch
		{
			// Ignore errors during cleanup
		}
	}
}
