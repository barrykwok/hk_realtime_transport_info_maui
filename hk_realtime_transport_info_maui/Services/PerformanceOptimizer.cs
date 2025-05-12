using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Memory;

namespace hk_realtime_transport_info_maui.Services
{
    /// <summary>
    /// Helper class for performance optimization, memory management, and monitoring
    /// </summary>
    public class PerformanceOptimizer
    {
        private readonly ILogger<PerformanceOptimizer> _logger;
        private readonly MemoryCache _metricCache;
        private readonly Dictionary<string, Stopwatch> _stopwatches = new();
        private readonly object _lockObject = new();
        
        /// <summary>
        /// Maximum number of entries to store in metric history
        /// </summary>
        private const int MaxMetricHistorySize = 100;
        
        public PerformanceOptimizer(ILogger<PerformanceOptimizer> logger)
        {
            _logger = logger;
            _metricCache = new MemoryCache(new MemoryCacheOptions());
        }
        
        /// <summary>
        /// Starts a timer for the specified operation
        /// </summary>
        public void StartTimer(string operationName)
        {
            lock (_lockObject)
            {
                if (_stopwatches.TryGetValue(operationName, out var existingTimer))
                {
                    existingTimer.Restart();
                }
                else
                {
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();
                    _stopwatches[operationName] = stopwatch;
                }
            }
        }
        
        /// <summary>
        /// Stops the timer for the specified operation and records the elapsed time
        /// </summary>
        public long StopTimer(string operationName)
        {
            lock (_lockObject)
            {
                if (_stopwatches.TryGetValue(operationName, out var stopwatch))
                {
                    stopwatch.Stop();
                    var elapsedMs = stopwatch.ElapsedMilliseconds;
                    
                    // Record the metric
                    RecordMetric(operationName, elapsedMs);
                    
                    return elapsedMs;
                }
                
                _logger?.LogWarning("Attempted to stop timer for operation '{operationName}' that was not started", operationName);
                return 0;
            }
        }
        
        /// <summary>
        /// Measures the execution time of the specified task
        /// </summary>
        public async Task<T> MeasureExecutionTimeAsync<T>(string operationName, Func<Task<T>> taskFunc)
        {
            StartTimer(operationName);
            try
            {
                return await taskFunc();
            }
            finally
            {
                var elapsed = StopTimer(operationName);
                _logger?.LogDebug("Operation '{operationName}' completed in {elapsed}ms", operationName, elapsed);
            }
        }
        
        /// <summary>
        /// Measures the execution time of the specified action
        /// </summary>
        public void MeasureExecutionTime(string operationName, Action action)
        {
            StartTimer(operationName);
            try
            {
                action();
            }
            finally
            {
                var elapsed = StopTimer(operationName);
                _logger?.LogDebug("Operation '{operationName}' completed in {elapsed}ms", operationName, elapsed);
            }
        }
        
        /// <summary>
        /// Records a metric for the specified operation
        /// </summary>
        private void RecordMetric(string metricName, long value)
        {
            var key = $"metric_{metricName}";
            
            if (_metricCache.TryGetValue(key, out List<long>? metricHistory) && metricHistory != null)
            {
                // Add the new value to the history
                metricHistory.Add(value);
                
                // Trim the history if it's too large
                if (metricHistory.Count > MaxMetricHistorySize)
                {
                    metricHistory.RemoveAt(0); // Remove the oldest entry
                }
            }
            else
            {
                // Create a new history list
                metricHistory = new List<long> { value };
                _metricCache.Set(key, metricHistory, TimeSpan.FromHours(1));
            }
        }
        
        /// <summary>
        /// Gets the average value for the specified metric over the recorded history
        /// </summary>
        public double GetAverageMetric(string metricName)
        {
            var key = $"metric_{metricName}";
            
            if (_metricCache.TryGetValue(key, out List<long>? metricHistory) && metricHistory != null && metricHistory.Count > 0)
            {
                return metricHistory.Average();
            }
            
            return 0;
        }
        
        /// <summary>
        /// Gets the 95th percentile value for the specified metric over the recorded history
        /// </summary>
        public double Get95thPercentile(string metricName)
        {
            var key = $"metric_{metricName}";
            
            if (_metricCache.TryGetValue(key, out List<long>? metricHistory) && metricHistory != null && metricHistory.Count > 0)
            {
                // Sort the values
                var sortedValues = metricHistory.OrderBy(x => x).ToList();
                
                // Calculate the index for the 95th percentile
                int index = (int)Math.Ceiling(sortedValues.Count * 0.95) - 1;
                if (index < 0) index = 0;
                
                return sortedValues[index];
            }
            
            return 0;
        }
        
        /// <summary>
        /// Gets the summary statistics for the specified metric
        /// </summary>
        public MetricSummary GetMetricSummary(string metricName)
        {
            var key = $"metric_{metricName}";
            
            if (_metricCache.TryGetValue(key, out List<long>? metricHistory) && metricHistory != null && metricHistory.Count > 0)
            {
                var sortedValues = metricHistory.OrderBy(x => x).ToList();
                int count = sortedValues.Count;
                
                return new MetricSummary
                {
                    MetricName = metricName,
                    Min = sortedValues.First(),
                    Max = sortedValues.Last(),
                    Average = sortedValues.Average(),
                    Median = sortedValues[count / 2],
                    Percentile95 = sortedValues[(int)Math.Ceiling(count * 0.95) - 1],
                    Count = count
                };
            }
            
            return new MetricSummary
            {
                MetricName = metricName,
                Count = 0
            };
        }
        
        /// <summary>
        /// Logs a memory usage report
        /// </summary>
        public void LogMemoryUsage()
        {
            GC.Collect(); // Force garbage collection to get accurate reading
            
            long totalMemory = GC.GetTotalMemory(true) / 1024 / 1024; // Convert to MB
            
            _logger?.LogInformation("Memory usage: {totalMemory} MB", totalMemory);
        }
        
        /// <summary>
        /// Optimize memory usage by trimming collections and forcing garbage collection
        /// </summary>
        public void OptimizeMemory()
        {
            try
            {
                _logger?.LogDebug("Starting memory optimization");
                
                // Force a full garbage collection
                GC.Collect(2, GCCollectionMode.Forced, true, true);
                
                // Wait for finalizers to complete
                GC.WaitForPendingFinalizers();
                
                // Final collection
                GC.Collect(2, GCCollectionMode.Forced, true, true);
                
                // Log memory usage after optimization
                long memoryAfter = GC.GetTotalMemory(true) / 1024 / 1024;
                _logger?.LogInformation("Memory after optimization: {memoryAfter} MB", memoryAfter);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during memory optimization");
            }
        }
        
        /// <summary>
        /// Calculates the ideal chunk size for UI virtualization based on device capabilities
        /// </summary>
        public int CalculateIdealChunkSize(int totalItems, int itemHeight, int viewportHeight)
        {
            try
            {
                // Get available memory in MB
                long availableMemory = GC.GetTotalMemory(false) / (1024 * 1024);
                
                // Base calculation on viewport
                int visibleItems = viewportHeight / itemHeight;
                
                // Calculate buffer (3x visible items is a good starting point)
                int bufferMultiplier = availableMemory > 200 ? 5 : (availableMemory > 100 ? 3 : 2);
                int idealChunkSize = visibleItems * bufferMultiplier;
                
                // Cap at totalItems
                idealChunkSize = Math.Min(idealChunkSize, totalItems);
                
                // Ensure minimum size
                idealChunkSize = Math.Max(idealChunkSize, 20);
                
                _logger?.LogDebug("Calculated ideal chunk size: {size} items for virtualization", idealChunkSize);
                return idealChunkSize;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error calculating ideal chunk size");
                return 50; // Default fallback
            }
        }
        
        /// <summary>
        /// Disable animations when loading large datasets to improve performance
        /// </summary>
        public void OptimizeForLargeDataset(bool enable)
        {
            try
            {
                if (enable)
                {
                    // Suggest disabling animations via reflection when loading large datasets
                    var animationAssembly = typeof(Microsoft.Maui.Controls.VisualElement).Assembly;
                    var animationProperty = animationAssembly
                        .GetType("Microsoft.Maui.Controls.VisualElement")
                        ?.GetProperty("DisableAnimations", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                    
                    animationProperty?.SetValue(null, true);
                    
                    _logger?.LogDebug("Disabled animations for large dataset loading");
                }
                else
                {
                    // Re-enable animations
                    var animationAssembly = typeof(Microsoft.Maui.Controls.VisualElement).Assembly;
                    var animationProperty = animationAssembly
                        .GetType("Microsoft.Maui.Controls.VisualElement")
                        ?.GetProperty("DisableAnimations", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                    
                    animationProperty?.SetValue(null, false);
                    
                    _logger?.LogDebug("Re-enabled animations after large dataset loading");
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error optimizing for large dataset");
            }
        }
        
        /// <summary>
        /// Clears all timer and metric data
        /// </summary>
        public void ClearMetrics()
        {
            lock (_lockObject)
            {
                _stopwatches.Clear();
                _metricCache.Dispose();
            }
        }
    }
    
    /// <summary>
    /// Summary statistics for a performance metric
    /// </summary>
    public class MetricSummary
    {
        public required string MetricName { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }
        public double Average { get; set; }
        public long Median { get; set; }
        public long Percentile95 { get; set; }
        public int Count { get; set; }
        
        public override string ToString()
        {
            return $"{MetricName}: Min={Min}ms, Max={Max}ms, Avg={Average:F1}ms, Median={Median}ms, P95={Percentile95}ms, Count={Count}";
        }
    }
} 