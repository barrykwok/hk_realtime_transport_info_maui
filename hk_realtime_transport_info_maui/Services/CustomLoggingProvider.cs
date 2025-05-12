using System;
using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace hk_realtime_transport_info_maui.Services
{
    public class CustomLoggingProvider : ILoggerProvider
    {
        private readonly ConcurrentDictionary<string, CustomLogger> _loggers = new();
        private readonly CustomLoggerConfiguration _config;

        public CustomLoggingProvider(CustomLoggerConfiguration config)
        {
            _config = config;
        }

        public ILogger CreateLogger(string categoryName)
        {
            return _loggers.GetOrAdd(categoryName, name => new CustomLogger(name, _config));
        }

        public void Dispose()
        {
            _loggers.Clear();
        }
    }

    public class CustomLogger : ILogger
    {
        private readonly string _name;
        private readonly CustomLoggerConfiguration _config;

        public CustomLogger(string name, CustomLoggerConfiguration config)
        {
            _name = name;
            _config = config;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => default!;

        public bool IsEnabled(LogLevel logLevel)
        {
            // Only log messages from our app namespaces
            bool isNamespaceAllowed = _name.StartsWith(_config.AppNamespace);

            // Filter by log level
            bool isLevelAllowed = logLevel >= _config.MinimumLogLevel;

            return isNamespaceAllowed && isLevelAllowed;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, 
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            string message = formatter(state, exception);

            // Skip empty messages
            if (string.IsNullOrEmpty(message))
                return;

            // Create formatted log entry
            var logBuilder = new StringBuilder();
            
            // Add timestamp
            logBuilder.Append($"{DateTime.Now:HH:mm:ss.fff} ");

            // Add app prefix to make logs easily identifiable
            logBuilder.Append("[APP] ");
            
            // Add level with appropriate formatting
            var (levelText, paddedLevel) = GetFormattedLogLevel(logLevel);
            logBuilder.Append(paddedLevel);
            
            // Add category name - shortened to just the class name
            string shortName = GetShortName(_name);
            logBuilder.Append($"[{shortName}] ");
            
            // Add message 
            logBuilder.Append(message);
            
            // Add exception if exists and if enabled in config
            if (exception != null && _config.IncludeExceptions)
            {
                // Add a newline before exception details
                logBuilder.AppendLine();
                
                // Handle exception message
                logBuilder.Append($"      Exception: {exception.Message}");
                
                // Add stack trace for Error and Critical levels only
                if (logLevel >= LogLevel.Error)
                {
                    logBuilder.AppendLine();
                    logBuilder.Append($"      {exception.StackTrace}");
                    
                    // For critical errors, include inner exceptions if present
                    if (logLevel == LogLevel.Critical && exception.InnerException != null)
                    {
                        logBuilder.AppendLine();
                        logBuilder.Append($"      Inner Exception: {exception.InnerException.Message}");
                        logBuilder.AppendLine();
                        logBuilder.Append($"      {exception.InnerException.StackTrace}");
                    }
                }
            }
            
            // Output to debug console
            System.Diagnostics.Debug.WriteLine(logBuilder.ToString());
        }

        private (string text, string formatted) GetFormattedLogLevel(LogLevel logLevel)
        {
            return logLevel switch
            {
                LogLevel.Trace => ("TRACE", "[TRACE] "),
                LogLevel.Debug => ("DEBUG", "[DEBUG] "),
                LogLevel.Information => ("INFO", "[INFO]  "),
                LogLevel.Warning => ("WARN", "[WARN]  "),
                LogLevel.Error => ("ERROR", "[ERROR] "),
                LogLevel.Critical => ("CRIT", "[CRIT]  "),
                _ => ("", "[      ]")
            };
        }
        
        private string GetShortName(string fullName)
        {
            // Extract just the class name from the full namespace
            string[] parts = fullName.Split('.');
            return parts.Length > 0 ? parts[^1] : fullName;
        }
    }

    public class CustomLoggerConfiguration
    {
        public string AppNamespace { get; set; } = "hk_realtime_transport_info_maui";
        public LogLevel MinimumLogLevel { get; set; } = LogLevel.Debug;
        public bool IncludeExceptions { get; set; } = true;
        
        // Maximum number of characters to log from a message
        public int MaxMessageLength { get; set; } = 8000;
    }
    
    public static class CustomLoggingExtensions
    {
        public static ILoggingBuilder AddCustomLogger(
            this ILoggingBuilder builder,
            Action<CustomLoggerConfiguration>? configure = null)
        {
            var config = new CustomLoggerConfiguration();
            configure?.Invoke(config);
            
            builder.AddProvider(new CustomLoggingProvider(config));
            return builder;
        }
    }
} 