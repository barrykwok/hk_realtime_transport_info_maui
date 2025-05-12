using Microsoft.Extensions.Logging;
using System.Linq;

namespace hk_realtime_transport_info_maui.Services
{
    /// <summary>
    /// Service that provides centralized exception handling for the application.
    /// </summary>
    public class ExceptionHandlingService
    {
        private readonly ILogger<ExceptionHandlingService> _logger;
        
        public ExceptionHandlingService(ILogger<ExceptionHandlingService> logger)
        {
            _logger = logger;
        }
        
        /// <summary>
        /// Safely executes an action and catches any exceptions that occur.
        /// </summary>
        /// <param name="action">The action to execute.</param>
        /// <param name="errorMessage">The error message to log if an exception occurs.</param>
        /// <returns>True if the action completed successfully, false otherwise.</returns>
        public bool TrySafeExecute(Action action, string errorMessage)
        {
            try
            {
                action();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, errorMessage);
                return false;
            }
        }
        
        /// <summary>
        /// Safely executes an async task and catches any exceptions that occur.
        /// </summary>
        /// <param name="task">The task to execute.</param>
        /// <param name="errorMessage">The error message to log if an exception occurs.</param>
        /// <returns>A task that completes with true if the action completed successfully, false otherwise.</returns>
        public async Task<bool> TrySafeExecuteAsync(Func<Task> task, string errorMessage)
        {
            try
            {
                await task();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, errorMessage);
                return false;
            }
        }
        
        /// <summary>
        /// Safely executes a function that returns a value and catches any exceptions that occur.
        /// </summary>
        /// <typeparam name="T">The return type of the function.</typeparam>
        /// <param name="func">The function to execute.</param>
        /// <param name="defaultValue">The default value to return if an exception occurs.</param>
        /// <param name="errorMessage">The error message to log if an exception occurs.</param>
        /// <returns>The result of the function or the default value if an exception occurs.</returns>
        public T TrySafeExecute<T>(Func<T> func, T defaultValue, string errorMessage)
        {
            try
            {
                return func();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, errorMessage);
                return defaultValue;
            }
        }
        
        /// <summary>
        /// Safely executes an async function that returns a value and catches any exceptions that occur.
        /// </summary>
        /// <typeparam name="T">The return type of the function.</typeparam>
        /// <param name="func">The async function to execute.</param>
        /// <param name="defaultValue">The default value to return if an exception occurs.</param>
        /// <param name="errorMessage">The error message to log if an exception occurs.</param>
        /// <returns>A task that completes with the result of the function or the default value if an exception occurs.</returns>
        public async Task<T> TrySafeExecuteAsync<T>(Func<Task<T>> func, T defaultValue, string errorMessage)
        {
            try
            {
                return await func();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, errorMessage);
                return defaultValue;
            }
        }
        
        /// <summary>
        /// Logs an exception with the provided error message.
        /// </summary>
        /// <param name="ex">The exception to log.</param>
        /// <param name="errorMessage">The error message to log.</param>
        public void LogException(Exception ex, string errorMessage)
        {
            _logger.LogError(ex, errorMessage);
        }

        /// <summary>
        /// Attempts to recover from SQLite errors.
        /// </summary>
        private void AttemptSQLiteRecovery()
        {
            try
            {
                _logger.LogInformation("Attempting to recover from SQLite error");
                
                // Get the database service and perform recovery operations
                if (IPlatformApplication.Current?.Services != null)
                {
                    var databaseService = IPlatformApplication.Current.Services.GetService<DatabaseService>();
                    if (databaseService != null)
                    {
                        // Detect if this is corruption or other SQLite issue
                        bool isCorrupted = false;
                        
                        // Common SQLite error messages that indicate corruption
                        string[] corruptionIndicators = new string[] 
                        { 
                            "database disk image is malformed",
                            "database is corrupt",
                            "database disk header is malformed",
                            "malformed database schema",
                            "index corruption",
                            "database corruption"
                        };
                        
                        // Check if any indicators are in the exception messages
                        if (LastException != null && corruptionIndicators.Any(indicator => 
                            LastException.Message.Contains(indicator) || 
                            (LastException.InnerException != null && LastException.InnerException.Message.Contains(indicator))))
                        {
                            _logger.LogWarning("Database corruption detected, attempting repair");
                            isCorrupted = true;
                        }
                        
                        // Use different recovery approaches depending on the issue type
                        if (isCorrupted)
                        {
                            // For corruption, try to repair database
                            bool repairSuccessful = databaseService.CheckAndRepairDatabase();
                            
                            if (repairSuccessful)
                            {
                                _logger.LogInformation("Database repair successful");
                            }
                            else
                            {
                                _logger.LogError("Database repair failed");
                            }
                        }
                        else
                        {
                            // For non-corruption issues, just try reconnecting
                            databaseService.CloseAndReopenConnection();
                            _logger.LogInformation("SQLite connection reset successfully");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to recover from SQLite error");
            }
        }
        
        // Store the last exception for diagnostics
        private Exception? LastException { get; set; }
        
        /// <summary>
        /// Handles an unhandled exception by logging it and displaying a user-friendly message.
        /// </summary>
        /// <param name="ex">The exception to handle.</param>
        /// <param name="showUI">Whether to display a UI message to the user.</param>
        /// <returns>True if the exception was handled successfully, false otherwise.</returns>
        public bool HandleUnhandledException(Exception ex, bool showUI = true)
        {
            try
            {
                // Store exception for diagnostics
                LastException = ex;
                
                // Log the exception
                _logger.LogCritical(ex, "Unhandled exception caught: {Message}", ex.Message);
                
                // Determine if this is a SQLite error
                bool isSQLiteError = IsSQLiteException(ex);
                string errorMessage = GetUserFriendlyErrorMessage(ex);
                
                // If it's a SQLite error, try to recover
                if (isSQLiteError)
                {
                    AttemptSQLiteRecovery();
                }
                
                // Show UI message if requested
                if (showUI)
                {
                    ShowErrorMessage(errorMessage);
                }
                
                return true;
            }
            catch (Exception innerEx)
            {
                // If we fail to handle the exception, log this failure too
                _logger.LogCritical(innerEx, "Failed to handle unhandled exception: {Message}", innerEx.Message);
                return false;
            }
        }
        
        /// <summary>
        /// Determines if an exception is related to SQLite.
        /// </summary>
        /// <param name="ex">The exception to check.</param>
        /// <returns>True if the exception is SQLite-related, false otherwise.</returns>
        private bool IsSQLiteException(Exception ex)
        {
            // Check the exception type or message for SQLite-specific information
            return ex.GetType().Name.Contains("SQLite") || 
                   ex.Message.Contains("SQLite") ||
                   (ex.InnerException != null && 
                    (ex.InnerException.GetType().Name.Contains("SQLite") || 
                     ex.InnerException.Message.Contains("SQLite")));
        }
        
        /// <summary>
        /// Gets a user-friendly error message for an exception.
        /// </summary>
        /// <param name="ex">The exception to get a message for.</param>
        /// <returns>A user-friendly error message.</returns>
        private string GetUserFriendlyErrorMessage(Exception ex)
        {
            // SQLite errors
            if (IsSQLiteException(ex))
            {
                return "A database error occurred. The app will try to recover automatically.";
            }
            
            // Network errors
            if (ex is System.Net.Http.HttpRequestException || 
                ex is System.Net.WebException || 
                ex.Message.Contains("network") || 
                ex.Message.Contains("connection"))
            {
                return "A network error occurred. Please check your internet connection and try again.";
            }
            
            // Out of memory errors
            if (ex is OutOfMemoryException)
            {
                return "The app has run out of memory. Please restart the app.";
            }
            
            // For other exceptions, provide a generic message
            return "An unexpected error occurred. The application will continue running but some features may be affected.";
        }
        
        /// <summary>
        /// Shows an error message to the user via the UI.
        /// </summary>
        /// <param name="message">The message to display.</param>
        private void ShowErrorMessage(string message)
        {
            try
            {
                // Display the error on the main thread
                MainThread.BeginInvokeOnMainThread(() =>
                {
                    try 
                    {
                        if (Application.Current != null)
                        {
                            var window = Application.Current.Windows.FirstOrDefault();
                            if (window?.Page != null)
                            {
                                window.Page.DisplayAlert("Application Error", message, "OK");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to display error alert");
                    }
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to display error message");
            }
        }
    }
} 