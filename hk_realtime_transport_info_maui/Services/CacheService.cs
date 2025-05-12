using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;

namespace hk_realtime_transport_info_maui.Services
{
    /// <summary>
    /// Options for configuring the CacheService
    /// </summary>
    public class CacheOptions
    {
        /// <summary>
        /// Default expiration time for cache entries
        /// </summary>
        public TimeSpan DefaultExpiration { get; set; } = TimeSpan.FromMinutes(5);
        
        /// <summary>
        /// Default sliding expiration time for cache entries
        /// </summary>
        public TimeSpan? DefaultSlidingExpiration { get; set; } = TimeSpan.FromMinutes(2);
        
        /// <summary>
        /// Size limit for the cache in entries
        /// </summary>
        public int SizeLimit { get; set; } = 1000;
        
        /// <summary>
        /// Flag to enable cache eviction notifications
        /// </summary>
        public bool EnableCacheEvents { get; set; } = true;
        
        /// <summary>
        /// Interval to scan for expired items
        /// </summary>
        public TimeSpan ExpirationScanFrequency { get; set; } = TimeSpan.FromMinutes(1);
        
        /// <summary>
        /// Maximum time an item can stay in cache regardless of access
        /// </summary>
        public TimeSpan? AbsoluteExpirationRelativeToNow { get; set; } = TimeSpan.FromHours(1);
    }

    /// <summary>
    /// Enhanced cache service that extends Microsoft's MemoryCache with additional functionality
    /// </summary>
    public class CacheService : IDisposable
    {
        private readonly IMemoryCache _cache;
        private readonly ILogger<CacheService>? _logger;
        private readonly CacheOptions _options;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _expirationTokens = new();
        private readonly ConcurrentDictionary<string, DateTime> _cacheEntryTimestamps = new();
        
        // Cache statistics
        private long _totalHits = 0;
        private long _totalMisses = 0;
        private long _totalEvictions = 0;
        
        // For cache cleanup
        private readonly Timer _cleanupTimer;
        private bool _disposed = false;

        public CacheService(IMemoryCache cache, ILogger<CacheService>? logger, IOptions<CacheOptions>? options = null)
        {
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _logger = logger;
            _options = options?.Value ?? new CacheOptions();
            
            // Set up cache cleanup timer with a state object to prevent nullability warning
            _cleanupTimer = new Timer(CleanupExpiredEntries, new object(), _options.ExpirationScanFrequency, _options.ExpirationScanFrequency);
            
            _logger?.LogInformation("CacheService initialized with Size Limit: {sizeLimit}, Default Expiration: {defaultExpiration}", 
                _options.SizeLimit, _options.DefaultExpiration);
        }

        /// <summary>
        /// Gets an item from the cache. Returns default if not found.
        /// </summary>
        public T? Get<T>(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
                
            // Intern string key for better memory usage
            key = string.Intern(key);
                
            if (_cache.TryGetValue(key, out T? value))
            {
                Interlocked.Increment(ref _totalHits);
                _logger?.LogDebug("Cache hit for key: {key}", key);
                return value;
            }
            
            Interlocked.Increment(ref _totalMisses);
            _logger?.LogDebug("Cache miss for key: {key}", key);
            return default;
        }
        
        /// <summary>
        /// Tries to get an item from the cache. Returns true if found.
        /// </summary>
        public bool TryGetValue<T>(string key, out T? value) where T : class
        {
            if (string.IsNullOrEmpty(key))
            {
                value = default!;
                return false;
            }

            if (_cache.TryGetValue(key, out object? cachedValue) && cachedValue is T typedValue)
            {
                value = typedValue;
                return true;
            }

            value = default!;
            return false;
        }

        /// <summary>
        /// Sets an item in the cache with optional expiration time.
        /// </summary>
        public void Set<T>(string key, T value, TimeSpan? expiration = null)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
                
            // Intern string key for better memory usage
            key = string.Intern(key);
                
            // Remove old entry if exists
            if (_expirationTokens.TryRemove(key, out var oldTokenSource))
            {
                oldTokenSource.Cancel();
                oldTokenSource.Dispose();
            }
            
            var options = new MemoryCacheEntryOptions
            {
                Size = 1, // Each entry costs 1 unit
                Priority = CacheItemPriority.Normal
            };
            
            // Apply expiration policy
            var expirationTime = expiration ?? _options.DefaultExpiration;
            options.AbsoluteExpirationRelativeToNow = expirationTime;
            
            // Apply sliding expiration if configured
            if (_options.DefaultSlidingExpiration.HasValue)
            {
                options.SlidingExpiration = _options.DefaultSlidingExpiration;
            }
            
            // Apply absolute expiration cap if configured
            if (_options.AbsoluteExpirationRelativeToNow.HasValue)
            {
                // Use the shorter of the two expirations
                if (expirationTime > _options.AbsoluteExpirationRelativeToNow.Value)
                {
                    options.AbsoluteExpirationRelativeToNow = _options.AbsoluteExpirationRelativeToNow;
                }
            }
            
            // Set up cache eviction callback
            if (_options.EnableCacheEvents)
            {
                options.RegisterPostEvictionCallback(OnCacheEntryEvicted);
            }
            
            // Create token source for manual expiration
            var tokenSource = new CancellationTokenSource();
            options.AddExpirationToken(new CancellationChangeToken(tokenSource.Token));
            _expirationTokens[key] = tokenSource;
            
            // Track when the entry was added
            _cacheEntryTimestamps[key] = DateTime.UtcNow;
            
            // Set in the cache
            _cache.Set(key, value, options);
            _logger?.LogDebug("Cache entry set for key: {key}, expires in {expiration}", key, expirationTime);
        }
        
        /// <summary>
        /// Sets an item in the cache using a factory function for lazy loading. Uses default expiration.
        /// </summary>
        public T? GetOrCreate<T>(string key, Func<T?> factory, TimeSpan? expiration = null) where T : class
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
                
            if (TryGetValue<T>(key, out var cachedValue) && cachedValue != null)
            {
                return cachedValue;
            }
            
            // Value not in cache, compute it
            var newValue = factory();
            
            // Only cache if not null
            if (newValue != null)
            {
                Set(key, newValue, expiration);
            }
            
            return newValue;
        }
        
        /// <summary>
        /// Sets an item in the cache using an async factory function for lazy loading. Uses default expiration.
        /// </summary>
        public async Task<T?> GetOrCreateAsync<T>(string key, Func<Task<T?>> factory, TimeSpan? expiration = null) where T : class
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
                
            if (TryGetValue<T>(key, out var cachedValue) && cachedValue != null)
            {
                return cachedValue;
            }
            
            // Value not in cache, compute it
            var newValue = await factory();
            
            // Only cache if not null
            if (newValue != null)
            {
                Set(key, newValue, expiration);
            }
            
            return newValue;
        }

        /// <summary>
        /// Removes an item from the cache.
        /// </summary>
        public void Remove(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
                
            _cache.Remove(key);
            
            // Clean up token source
            if (_expirationTokens.TryRemove(key, out var tokenSource))
            {
                tokenSource.Cancel();
                tokenSource.Dispose();
            }
            
            // Remove from timestamps
            _cacheEntryTimestamps.TryRemove(key, out _);
            
            _logger?.LogDebug("Cache entry removed for key: {key}", key);
        }
        
        /// <summary>
        /// Removes all items matching the specified pattern from the cache.
        /// </summary>
        public void RemoveByPattern(string pattern)
        {
            if (string.IsNullOrEmpty(pattern))
                throw new ArgumentNullException(nameof(pattern));
            
            var keysToRemove = new List<string>();
            
            // Find keys matching the pattern
            foreach (var key in _cacheEntryTimestamps.Keys)
            {
                if (key.Contains(pattern))
                {
                    keysToRemove.Add(key);
                }
            }
            
            // Remove matching keys
            foreach (var key in keysToRemove)
            {
                Remove(key);
            }
            
            _logger?.LogInformation("Removed {count} cache entries matching pattern: {pattern}", keysToRemove.Count, pattern);
        }
        
        /// <summary>
        /// Clears all items from the cache.
        /// </summary>
        public void Clear()
        {
            // Get all keys
            var allKeys = new List<string>(_expirationTokens.Keys);
            
            // Remove each key
            foreach (var key in allKeys)
            {
                Remove(key);
            }
            
            _logger?.LogInformation("Cache cleared - {count} entries removed", allKeys.Count);
        }
        
        /// <summary>
        /// Gets cache statistics.
        /// </summary>
        public CacheStatistics GetStatistics()
        {
            return new CacheStatistics
            {
                TotalHits = Interlocked.Read(ref _totalHits),
                TotalMisses = Interlocked.Read(ref _totalMisses),
                TotalEvictions = Interlocked.Read(ref _totalEvictions),
                CurrentItemCount = _cacheEntryTimestamps.Count,
                HitRatio = (Interlocked.Read(ref _totalHits) + Interlocked.Read(ref _totalMisses)) > 0 
                    ? (double)Interlocked.Read(ref _totalHits) / (Interlocked.Read(ref _totalHits) + Interlocked.Read(ref _totalMisses)) 
                    : 0
            };
        }
        
        /// <summary>
        /// Statistics about the cache performance.
        /// </summary>
        public class CacheStatistics
        {
            public long TotalHits { get; set; }
            public long TotalMisses { get; set; }
            public long TotalEvictions { get; set; }
            public int CurrentItemCount { get; set; }
            public double HitRatio { get; set; }
            
            public override string ToString()
            {
                return $"Hits: {TotalHits}, Misses: {TotalMisses}, Evictions: {TotalEvictions}, Items: {CurrentItemCount}, Hit Ratio: {HitRatio:P2}";
            }
        }
        
        /// <summary>
        /// Handles cache entry eviction events.
        /// </summary>
        private void OnCacheEntryEvicted(object key, object? value, EvictionReason reason, object? state)
        {
            Interlocked.Increment(ref _totalEvictions);
            
            var keyString = key.ToString();
            
            // Clean up resources for the evicted item
            if (keyString != null && _expirationTokens.TryRemove(keyString, out var tokenSource))
            {
                tokenSource.Dispose();
            }
            
            if (keyString != null)
            {
                _cacheEntryTimestamps.TryRemove(keyString, out _);
            }
            
            _logger?.LogDebug("Cache entry evicted: {key}, Reason: {reason}", keyString, reason);
        }
        
        /// <summary>
        /// Periodically scans for and removes expired cache entries.
        /// </summary>
        private void CleanupExpiredEntries(object? state)
        {
            if (_disposed) return;
            
            try
            {
                var now = DateTime.UtcNow;
                var keysToCheck = new List<string>(_cacheEntryTimestamps.Keys);
                var expiredCount = 0;
                
                foreach (var key in keysToCheck)
                {
                    // Skip if the key is no longer in the cache
                    if (!_cacheEntryTimestamps.TryGetValue(key, out var timestamp))
                        continue;
                    
                    // Check if the entry has exceeded the absolute maximum lifetime
                    if (_options.AbsoluteExpirationRelativeToNow.HasValue && 
                        (now - timestamp) > _options.AbsoluteExpirationRelativeToNow.Value)
                    {
                        Remove(key);
                        expiredCount++;
                    }
                }
                
                if (expiredCount > 0)
                {
                    _logger?.LogDebug("Cleanup timer removed {count} expired cache entries", expiredCount);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error during cache cleanup");
            }
        }
        
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;
                
            if (disposing)
            {
                _cleanupTimer?.Dispose();
                
                // Dispose all cancellation token sources
                foreach (var tokenSource in _expirationTokens.Values)
                {
                    tokenSource.Dispose();
                }
                
                _expirationTokens.Clear();
                _cacheEntryTimestamps.Clear();
            }
            
            _disposed = true;
        }
        
        ~CacheService()
        {
            Dispose(false);
        }
    }
} 