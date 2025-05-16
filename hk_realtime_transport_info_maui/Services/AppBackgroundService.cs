using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using hk_realtime_transport_info_maui.Services;

namespace hk_realtime_transport_info_maui.Services
{
    /// <summary>
    /// Background service to handle scheduled app maintenance tasks
    /// </summary>
    public class AppBackgroundService
    {
        private readonly LiteDbService _databaseService;
        private readonly ILogger<AppBackgroundService> _logger;
        private CancellationTokenSource? _cancellationTokenSource;
        private bool _isRunning;
        
        // Interval for database optimization (once per day when app is idle)
        private readonly TimeSpan _optimizationInterval = TimeSpan.FromDays(1);
        
        // Flag to control whether database maintenance should run automatically
        public bool EnableAutomaticMaintenance { get; set; } = false; // Disabled by default
        
        public AppBackgroundService(LiteDbService databaseService, ILogger<AppBackgroundService> logger)
        {
            _databaseService = databaseService;
            _logger = logger;
        }
        
        /// <summary>
        /// Starts the background service
        /// </summary>
        public void Start()
        {
            if (_isRunning)
                return;
                
            _isRunning = true;
            _cancellationTokenSource = new CancellationTokenSource();
            
            // Start maintenance task
            Task.Run(() => RunMaintenanceAsync(_cancellationTokenSource.Token));
            
            _logger?.LogInformation("Background service started");
        }
        
        /// <summary>
        /// Stops the background service
        /// </summary>
        public void Stop()
        {
            if (!_isRunning)
                return;
                
            _cancellationTokenSource?.Cancel();
            _isRunning = false;
            
            _logger?.LogInformation("Background service stopped");
        }
        
        /// <summary>
        /// Main maintenance loop that runs periodic tasks
        /// </summary>
        private async Task RunMaintenanceAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Wait for a short while after app start to let critical startup complete
                await Task.Delay(TimeSpan.FromMinutes(2), cancellationToken);
                
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // Only run database optimization if explicitly enabled
                        if (EnableAutomaticMaintenance)
                        {
                            // Perform database optimization during idle time
                            RunDatabaseOptimization();
                        }
                        else
                        {
                            _logger?.LogDebug("Skipping database maintenance as it is disabled");
                        }
                        
                        // Wait until next scheduled maintenance
                        await Task.Delay(_optimizationInterval, cancellationToken);
                    }
                    catch (TaskCanceledException)
                    {
                        // Normal cancellation
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error in maintenance task");
                        // Keep running but wait a bit before retry
                        await Task.Delay(TimeSpan.FromMinutes(30), cancellationToken);
                    }
                }
            }
            catch (TaskCanceledException)
            {
                // Normal cancellation 
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Background service stopped due to error");
            }
            finally
            {
                _isRunning = false;
            }
        }
        
        /// <summary>
        /// Runs database maintenance
        /// </summary>
        private void RunDatabaseOptimization()
        {
            try
            {
                _logger?.LogInformation("Starting scheduled database maintenance");
                
                // Run database maintenance (formerly analysis and optimization)
                _databaseService.AnalyzeAndOptimizeDatabase();
                
                _logger?.LogInformation("Scheduled database maintenance completed");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during database maintenance in background");
            }
        }
        
        /// <summary>
        /// Manually triggers database maintenance
        /// </summary>
        public async Task TriggerDatabaseOptimizationAsync()
        {
            await Task.Run(() => RunDatabaseOptimization());
        }
    }
} 