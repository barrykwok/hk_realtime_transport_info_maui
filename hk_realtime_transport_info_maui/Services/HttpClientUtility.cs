using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Caching.Memory;

namespace hk_realtime_transport_info_maui.Services
{
    /// <summary>
    /// Advanced HTTP client utility that provides:
    /// - Automatic gzip compression
    /// - Retry mechanism for poor network conditions
    /// - Limiting concurrent requests
    /// - Request prioritization and queuing
    /// </summary>
    public class HttpClientUtility
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<HttpClientUtility> _logger;
        private readonly SemaphoreSlim _concurrencyLimiter;
        private readonly PriorityQueue<QueuedRequest, int> _requestQueue;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task _processingTask;
        private readonly JsonSerializerOptions _jsonOptions;
        private readonly IMemoryCache _memoryCache;
        private readonly bool _enableResponseCaching;
        private readonly ExceptionHandlingService? _exceptionHandler;
        private bool _isProcessing = true;
        
        // Rate limiting
        private readonly SemaphoreSlim _rateLimiter = new SemaphoreSlim(1, 1);
        private DateTime _lastRequestTime = DateTime.MinValue;
        private const int MIN_DELAY_BETWEEN_REQUESTS_MS = 100; // 10 requests per second max

        // Default settings
        private readonly int _maxConcurrentRequests;
        private readonly int _maxRetryAttempts;
        private readonly int _initialRetryDelayMs;
        private readonly int _maxRetryDelayMs;
        private readonly TimeSpan _cacheExpiration = TimeSpan.FromSeconds(30); // Cache for 30 seconds

        // Constants for request priorities (lower number = higher priority)
        public const int PRIORITY_HIGH = 0;
        public const int PRIORITY_NORMAL = 50;
        public const int PRIORITY_LOW = 100;
        
        // Browser User-Agent strings to randomize requests
        private static readonly string[] _browserUserAgents = new[]
        {
            // Windows Chrome
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            
            // Mac Chrome
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            
            // Linux Chrome
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            
            // Windows Firefox
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            
            // Mac Firefox
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
            
            // Windows Edge
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/121.0.0.0 Safari/537.36",
            
            // Mac Safari
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            
            // Mobile - iOS Safari
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
            
            // Mobile - Android Chrome
            "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 12; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36",
            
            // Android Firefox
            "Mozilla/5.0 (Android 14; Mobile; rv:123.0) Gecko/123.0 Firefox/123.0",
            "Mozilla/5.0 (Android 13; Mobile; rv:122.0) Gecko/122.0 Firefox/122.0",
            
            // Hong Kong specific browsers might be good to include
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/116.0.5845.90 Mobile/15E148 Safari/604.1",
        };
        
        private static readonly Random _random = new Random();
        
        private static string GetRandomUserAgent()
        {
            int index = _random.Next(_browserUserAgents.Length);
            return _browserUserAgents[index];
        }

        public HttpClientUtility(ILogger<HttpClientUtility> logger, IMemoryCache memoryCache, HttpClientUtilityOptions? options = null, ExceptionHandlingService? exceptionHandler = null)
        {
            _logger = logger;
            _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
            _exceptionHandler = exceptionHandler;
            options ??= new HttpClientUtilityOptions();

            // Initialize settings
            _maxConcurrentRequests = options.MaxConcurrentRequests;
            _maxRetryAttempts = options.MaxRetryAttempts;
            _initialRetryDelayMs = options.InitialRetryDelayMs;
            _maxRetryDelayMs = options.MaxRetryDelayMs;
            _enableResponseCaching = options.EnableResponseCaching;

            // Create HTTP client with handler that supports gzip
            var handler = new SocketsHttpHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                PooledConnectionLifetime = TimeSpan.FromMinutes(2),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(1),
                MaxConnectionsPerServer = options.MaxConcurrentRequests * 2,
                EnableMultipleHttp2Connections = options.EnableHttp2
            };

            _httpClient = new HttpClient(handler);
            
            if (options.EnableCompression)
            {
                _httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
                _httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            }
            
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            _httpClient.Timeout = TimeSpan.FromSeconds(options.TimeoutSeconds);

            // Set up concurrency control and queue
            _concurrencyLimiter = new SemaphoreSlim(_maxConcurrentRequests);
            _requestQueue = new PriorityQueue<QueuedRequest, int>();
            _cancellationTokenSource = new CancellationTokenSource();

            // Configure JSON serialization options
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            // Start the background task that processes the queue
            _processingTask = Task.Run(ProcessQueueAsync);
        }

        /// <summary>
        /// Enqueues a GET request with the specified priority
        /// </summary>
        public Task<HttpResponseMessage> GetAsync(string url, int priority = PRIORITY_NORMAL, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            return EnqueueRequestAsync(HttpMethod.Get, url, null, headers, priority, cancellationToken);
        }

        /// <summary>
        /// Enqueues a GET request and deserializes the JSON response
        /// </summary>
        public async Task<T?> GetFromJsonAsync<T>(string url, int priority = PRIORITY_NORMAL, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            if (_exceptionHandler != null)
            {
                return await _exceptionHandler.TrySafeExecuteAsync(
                    async () =>
                    {
                        var response = await EnqueueRequestAsync(HttpMethod.Get, url, null, headers, priority, cancellationToken);
                        
                        if (response.IsSuccessStatusCode)
                        {
                            return await response.Content.ReadFromJsonAsync<T>(_jsonOptions, cancellationToken);
                        }
                        
                        return default;
                    },
                    default,
                    $"Error fetching and deserializing JSON from {url}");
            }
            else
            {
                try
                {
                    var response = await EnqueueRequestAsync(HttpMethod.Get, url, null, headers, priority, cancellationToken);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        return await response.Content.ReadFromJsonAsync<T>(_jsonOptions, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error fetching and deserializing JSON from {url}", url);
                }
                
                return default;
            }
        }

        /// <summary>
        /// Enqueues a POST request with the specified priority
        /// </summary>
        public Task<HttpResponseMessage> PostAsync(string url, HttpContent? content, int priority = PRIORITY_NORMAL, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            return EnqueueRequestAsync(HttpMethod.Post, url, content, headers, priority, cancellationToken);
        }

        /// <summary>
        /// Enqueues a POST request with JSON content and deserializes the JSON response
        /// </summary>
        public async Task<TResponse?> PostAsJsonAsync<TRequest, TResponse>(string url, TRequest data, int priority = PRIORITY_NORMAL, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            var content = JsonContent.Create(data, null, _jsonOptions);
            var response = await EnqueueRequestAsync(HttpMethod.Post, url, content, headers, priority, cancellationToken);
            
            if (response.IsSuccessStatusCode)
            {
                return await response.Content.ReadFromJsonAsync<TResponse>(_jsonOptions, cancellationToken);
            }
            
            return default;
        }

        /// <summary>
        /// Enqueues a PUT request with the specified priority
        /// </summary>
        public Task<HttpResponseMessage> PutAsync(string url, HttpContent? content, int priority = PRIORITY_NORMAL, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            return EnqueueRequestAsync(HttpMethod.Put, url, content, headers, priority, cancellationToken);
        }

        /// <summary>
        /// Enqueues a DELETE request with the specified priority
        /// </summary>
        public Task<HttpResponseMessage> DeleteAsync(string url, int priority = PRIORITY_NORMAL, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            return EnqueueRequestAsync(HttpMethod.Delete, url, null, headers, priority, cancellationToken);
        }

        /// <summary>
        /// Enqueues a request with the specified priority
        /// </summary>
        private Task<HttpResponseMessage> EnqueueRequestAsync(
            HttpMethod method, 
            string url, 
            HttpContent? content = null, 
            Dictionary<string, string>? headers = null, 
            int priority = PRIORITY_NORMAL, 
            CancellationToken cancellationToken = default)
        {
            if (!_isProcessing)
            {
                throw new InvalidOperationException("HttpClientUtility has been disposed");
            }
            
            // Check if the response is in cache
            if (_enableResponseCaching && method == HttpMethod.Get)
            {
                string cacheKey = $"HttpClientUtility_{method}_{url}";
                if (_memoryCache.TryGetValue(cacheKey, out HttpResponseMessage? cachedResponse))
                {
                    _logger.LogDebug("Cache hit for {method} {url}", method, url);
                    return Task.FromResult(cachedResponse!);
                }
            }
            
            var tcs = new TaskCompletionSource<HttpResponseMessage>();
            
            // Create the request
            var request = new QueuedRequest
            {
                Method = method,
                Url = url,
                Content = content,
                Headers = headers,
                CompletionSource = tcs,
                CancellationToken = cancellationToken,
                EnqueueTime = DateTime.UtcNow,
                Priority = priority
            };
            
            // Add to the queue
            lock (_requestQueue)
            {
                _requestQueue.Enqueue(request, priority);
            }
            
            return tcs.Task;
        }

        /// <summary>
        /// Background task that processes the request queue
        /// </summary>
        private async Task ProcessQueueAsync()
        {
            _logger.LogInformation("Request queue processor started");
            
            try
            {
                while (_isProcessing && !_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    QueuedRequest? request = null;
                    
                    // Get a request from the queue
                    lock (_requestQueue)
                    {
                        if (_requestQueue.Count > 0)
                        {
                            request = _requestQueue.Dequeue();
                        }
                    }
                    
                    if (request != null)
                    {
                        // Process the request
                        _ = Task.Run(() => ProcessRequestAsync(request), _cancellationTokenSource.Token);
                        
                        // Check if this is a high priority request
                        bool isHighPriority = request.Priority <= PRIORITY_HIGH;
                        
                        // Apply rate limiting
                        await ApplyRateLimitingAsync(isHighPriority);
                    }
                    else
                    {
                        // No requests in the queue, wait a bit
                        
                    }
                }
            }
            catch (TaskCanceledException)
            {
                _logger.LogInformation("Request queue processor was cancelled");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in request queue processor");
            }
            finally
            {
                _logger.LogInformation("Request queue processor stopped");
            }
        }
        
        private async Task ApplyRateLimitingAsync(bool isHighPriority)
        {
            try
            {
                await _rateLimiter.WaitAsync();
                
                var elapsed = DateTime.UtcNow - _lastRequestTime;
                var minDelay = isHighPriority 
                    ? TimeSpan.FromMilliseconds(MIN_DELAY_BETWEEN_REQUESTS_MS / 2)  // Half delay for high priority
                    : TimeSpan.FromMilliseconds(MIN_DELAY_BETWEEN_REQUESTS_MS);
                
                if (elapsed < minDelay)
                {
                    var delayTime = minDelay - elapsed;
                    await Task.Delay(delayTime); // Actually delay to prevent API rate limiting
                    _logger?.LogDebug("Rate limiting applied: delayed request by {delayMs}ms", delayTime.TotalMilliseconds);
                }
                
                _lastRequestTime = DateTime.UtcNow;
            }
            finally
            {
                _rateLimiter.Release();
            }
        }

        /// <summary>
        /// Processes a single request, acquiring a concurrency slot and handling retries
        /// </summary>
        private async Task ProcessRequestAsync(QueuedRequest request)
        {
            try
            {
                // Wait for a concurrency slot (this limits concurrent requests)
                await _concurrencyLimiter.WaitAsync(request.CancellationToken);
                
                try
                {
                    TimeSpan queueDelay = DateTime.UtcNow - request.EnqueueTime;
                    
                    // Log excessive delays for diagnostics
                    if (queueDelay.TotalSeconds > 5)
                    {
                        _logger.LogWarning("Processing {method} request to {url} after {delaySeconds}s in queue", 
                            request.Method, request.Url, queueDelay.TotalSeconds);
                    }
                    else 
                    {
                        _logger.LogDebug("Processing {method} request to {url} after {delayMs}ms in queue", 
                            request.Method, request.Url, queueDelay.TotalMilliseconds);
                    }
                    
                    // Execute the request with retry logic
                    var response = await ExecuteWithRetryAsync(request);
                    
                    // Complete the task with the result
                    request.CompletionSource.SetResult(response);
                }
                finally
                {
                    // Release the concurrency slot
                    _concurrencyLimiter.Release();
                }
            }
            catch (OperationCanceledException)
            {
                // Request was canceled
                request.CompletionSource.SetCanceled();
                _logger.LogDebug("Request to {url} was canceled", request.Url);
            }
            catch (Exception ex)
            {
                // Set the exception on the task
                request.CompletionSource.SetException(ex);
                _logger.LogError(ex, "Error processing request to {url}", request.Url);
            }
        }

        /// <summary>
        /// Executes a request with retry logic
        /// </summary>
        private async Task<HttpResponseMessage> ExecuteWithRetryAsync(QueuedRequest request)
        {
            int retryCount = 0;
            int delay = _initialRetryDelayMs;
            
            while (true)
            {
                try
                {
                    // Create a new request message for each attempt
                    var requestMessage = new HttpRequestMessage(request.Method, request.Url);
                    
                    // Set content if provided
                    if (request.Content != null)
                    {
                        requestMessage.Content = request.Content;
                    }
                    
                    // Set headers if provided
                    if (request.Headers != null)
                    {
                        foreach (var header in request.Headers)
                        {
                            requestMessage.Headers.TryAddWithoutValidation(header.Key, header.Value);
                        }
                    }
                    
                    // Set priority header for debugging/tracing purposes
                    AddPriorityHeader(requestMessage, request.Priority);
                    
                    // Add a random user agent
                    requestMessage.Headers.UserAgent.ParseAdd(GetRandomUserAgent());
                    
                    var startTime = DateTime.UtcNow;
                    
                    // Execute the request with the remaining time from the original timeout
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                        request.CancellationToken, 
                        _cancellationTokenSource.Token);
                        
                    // Set a timeout for this specific request
                    linkedCts.CancelAfter(_httpClient.Timeout);
                    
                    var response = await _httpClient.SendAsync(requestMessage, HttpCompletionOption.ResponseContentRead, linkedCts.Token);
                    
                    var requestDuration = DateTime.UtcNow - startTime;
                    
                    // Log success
                    _logger?.LogDebug("HTTP {method} {url} completed in {duration}ms with status {statusCode}",
                        request.Method, request.Url, requestDuration.TotalMilliseconds, response.StatusCode);
                    
                    if (!response.IsSuccessStatusCode && ShouldRetry(response.StatusCode))
                    {
                        if (retryCount < _maxRetryAttempts)
                        {
                            // Dispose the response before retrying
                            response.Dispose();
                            
                            retryCount++;
                            _logger?.LogWarning("Retrying request due to unsuccessful status code {statusCode} (attempt {attemptCount}/{maxAttempts}): {url}",
                                response.StatusCode, retryCount, _maxRetryAttempts, request.Url);
                            
                            // Wait before retrying with exponential backoff
                            await Task.Delay(delay);
                            delay = Math.Min(delay * 2, _maxRetryDelayMs);
                            continue;
                        }
                        
                        _logger?.LogError("HTTP request failed after {attemptCount} attempts: {url}, Status: {statusCode}",
                            retryCount + 1, request.Url, response.StatusCode);
                    }
                    
                    return response;
                }
                catch (HttpRequestException ex)
                {
                    if (retryCount < _maxRetryAttempts && IsTransientError(ex))
                    {
                        retryCount++;
                        _logger?.LogWarning(ex, "Transient error on attempt {attemptNumber}, retrying after {delay}ms: {url}", 
                            retryCount, delay, request.Url);
                        
                        // Wait before retrying with exponential backoff
                        await Task.Delay(delay);
                        delay = Math.Min(delay * 2, _maxRetryDelayMs);
                        continue;
                    }
                    
                    _logger?.LogError(ex, "HTTP request failed after {attemptNumber} attempts: {url}, Status: {status}", 
                        retryCount + 1, request.Url, ex.StatusCode);
                    throw;
                }
                catch (TaskCanceledException ex)
                {
                    if (retryCount < _maxRetryAttempts)
                    {
                        retryCount++;
                        _logger?.LogWarning(ex, "Request timeout on attempt {attemptNumber}, retrying after {delay}ms: {url}", 
                            retryCount, delay, request.Url);
                        
                        // Wait before retrying with exponential backoff
                        await Task.Delay(delay);
                        delay = Math.Min(delay * 2, _maxRetryDelayMs);
                        continue;
                    }
                    
                    _logger?.LogError(ex, "HTTP request timed out after {attemptNumber} attempts: {url}", 
                        retryCount + 1, request.Url);
                    throw;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Unexpected error executing HTTP request to {url}: {message}", 
                        request.Url, ex.Message);
                        
                    // Don't retry on unexpected exceptions
                    if (_exceptionHandler != null)
                    {
                        _exceptionHandler.LogException(ex, $"Unhandled exception in HTTP request to {request.Url}");
                    }
                    throw;
                }
            }
        }

        /// <summary>
        /// Determines if a request should be retried based on the HTTP status code
        /// </summary>
        private bool ShouldRetry(HttpStatusCode statusCode)
        {
            // Retry server errors, rate limiting, gateway timeouts, and forbidden (often used for rate limiting)
            return statusCode == HttpStatusCode.InternalServerError ||
                   statusCode == HttpStatusCode.BadGateway ||
                   statusCode == HttpStatusCode.ServiceUnavailable ||
                   statusCode == HttpStatusCode.GatewayTimeout ||
                   statusCode == HttpStatusCode.Forbidden ||
                   statusCode == HttpStatusCode.TooManyRequests;
        }

        /// <summary>
        /// Adds a priority header to an HTTP request
        /// </summary>
        /// <param name="request">The HTTP request to add the header to</param>
        /// <param name="priority">The priority level (use PRIORITY_* constants)</param>
        public static void AddPriorityHeader(HttpRequestMessage request, int priority)
        {
            // Add a custom X-Priority header with the priority value
            // This can be used for debugging or logging purposes
            request.Headers.TryAddWithoutValidation("X-Priority", priority.ToString());
            
            // Add a random User-Agent to emulate a browser request
            request.Headers.UserAgent.Clear();
            request.Headers.UserAgent.ParseAdd(GetRandomUserAgent());
        }

        /// <summary>
        /// Disposes resources used by the HTTP client utility
        /// </summary>
        public void Dispose()
        {
            _isProcessing = false;
            _cancellationTokenSource.Cancel();
            _cancellationTokenSource.Dispose();
            _httpClient.Dispose();
            _concurrencyLimiter.Dispose();
        }

        /// <summary>
        /// Represents a queued HTTP request
        /// </summary>
        private class QueuedRequest
        {
            public HttpMethod Method { get; set; } = HttpMethod.Get;
            public string Url { get; set; } = string.Empty;
            public HttpContent? Content { get; set; }
            public Dictionary<string, string>? Headers { get; set; }
            public TaskCompletionSource<HttpResponseMessage> CompletionSource { get; set; } = null!;
            public CancellationToken CancellationToken { get; set; }
            public DateTime EnqueueTime { get; set; }
            public int Priority { get; set; } = PRIORITY_NORMAL;
        }

        // Add a new method for optimized GET requests using the enhanced CacheService
        public async Task<string> GetWithOptimizedCachingAsync(string url, bool useCache = true, int cacheDurationSeconds = 60, CacheService? cacheService = null)
        {
            if (string.IsNullOrEmpty(url))
            {
                throw new ArgumentException("URL cannot be null or empty", nameof(url));
            }

            // Create cache key based on the URL - normalize the URL to avoid duplicate caching
            string normalizedUrl = url.TrimEnd('/');
            string cacheKey = $"http_response_{normalizedUrl}";
            
            // Check if a CacheService was provided
            if (useCache)
            {
                // Try to get from enhanced cache first if available
                if (cacheService != null)
                {
                    if (cacheService.TryGetValue(cacheKey, out string? cachedResponse) && cachedResponse != null)
                    {
                        _logger?.LogDebug("Retrieved HTTP response from enhanced cache for URL: {url}", url);
                        return cachedResponse;
                    }
                }
                // Fall back to standard memory cache if no CacheService was provided
                else if (_memoryCache != null && _memoryCache.TryGetValue(cacheKey, out string? cachedResponse) && cachedResponse != null)
                {
                    _logger?.LogDebug("Retrieved HTTP response from memory cache for URL: {url}", url);
                    return cachedResponse;
                }
            }
            
            // Use semaphore to limit concurrent requests
            await _rateLimiter.WaitAsync();
            
            try
            {
                // Retry logic 
                int retryCount = 0;
                int delay = _initialRetryDelayMs;
                
                while (true)
                {
                    try
                    {
                        // Set timeout
                        using var cts = new CancellationTokenSource();
                        cts.CancelAfter(TimeSpan.FromSeconds(_maxRetryDelayMs));
                        
                        // Make the request
                        var response = await _httpClient.GetAsync(url, cts.Token);
                        
                        // Check status
                        response.EnsureSuccessStatusCode();
                        
                        // Read content
                        string content = await response.Content.ReadAsStringAsync();
                        
                        // Cache the response if caching is enabled and we got valid content
                        if (useCache && !string.IsNullOrEmpty(content))
                        {
                            var cacheExpiration = TimeSpan.FromSeconds(cacheDurationSeconds);
                            
                            // Use enhanced cache service if provided
                            if (cacheService != null)
                            {
                                cacheService.Set(cacheKey, content, cacheExpiration);
                                _logger?.LogDebug("Cached HTTP response in enhanced cache for URL: {url} (expires in {seconds}s)", 
                                    url, cacheDurationSeconds);
                            }
                            // Fall back to standard memory cache
                            else if (_memoryCache != null)
                            {
                                var cacheOptions = new MemoryCacheEntryOptions()
                                    .SetAbsoluteExpiration(cacheExpiration)
                                    .SetSize(1); // Set size to 1 unit (required when using SizeLimit)
                                    
                                _memoryCache.Set(cacheKey, content, cacheOptions);
                                _logger?.LogDebug("Cached HTTP response in memory cache for URL: {url} (expires in {seconds}s)", 
                                    url, cacheDurationSeconds);
                            }
                        }
                        
                        return content;
                    }
                    catch (HttpRequestException ex)
                    {
                        if (retryCount < _maxRetryAttempts && IsTransientError(ex))
                        {
                            retryCount++;
                            _logger?.LogWarning(ex, "Transient error on attempt {attemptNumber}, retrying after {delay}ms: {url}", 
                                retryCount, delay, url);
                            
                            // Wait before retrying with exponential backoff
                            await Task.Delay(delay);
                            delay = Math.Min(delay * 2, _maxRetryDelayMs);
                            continue;
                        }
                        
                        _logger?.LogError(ex, "HTTP request failed after {attemptNumber} attempts: {url}, Status: {status}", 
                            retryCount + 1, url, ex.StatusCode);
                        throw;
                    }
                    catch (TaskCanceledException ex)
                    {
                        if (retryCount < _maxRetryAttempts)
                        {
                            retryCount++;
                            _logger?.LogWarning(ex, "Request timeout on attempt {attemptNumber}, retrying after {delay}ms: {url}", 
                                retryCount, delay, url);
                            
                            // Wait before retrying with exponential backoff
                            await Task.Delay(delay);
                            delay = Math.Min(delay * 2, _maxRetryDelayMs);
                            continue;
                        }
                        
                        _logger?.LogError(ex, "HTTP request timed out after {attemptNumber} attempts: {url}", 
                            retryCount + 1, url);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Unexpected error making HTTP request: {url}", url);
                        throw;
                    }
                }
            }
            finally
            {
                _rateLimiter.Release();
            }
        }
        
        // Helper method to determine if an error is transient
        private bool IsTransientError(HttpRequestException ex)
        {
            // Check for status codes that indicate a transient error (503, 504, etc.)
            if (ex.StatusCode.HasValue)
            {
                int statusCode = (int)ex.StatusCode.Value;
                return statusCode >= 500 || statusCode == 429; // 5xx server errors or 429 Too Many Requests
            }
            
            // Check if the exception message contains indications of network issues
            string message = ex.Message.ToLowerInvariant();
            return message.Contains("timeout") || 
                   message.Contains("connection") || 
                   message.Contains("reset") ||
                   message.Contains("network") ||
                   message.Contains("unreachable");
        }

        /// <summary>
        /// Performs multiple HTTP requests in a batch for better efficiency
        /// </summary>
        public async Task<List<(string url, HttpResponseMessage response)>> BatchGetAsync(
            IEnumerable<string> urls, 
            int priority = PRIORITY_NORMAL, 
            CancellationToken cancellationToken = default)
        {
            var tasks = new List<Task<HttpResponseMessage>>();
            var urlList = urls.ToList();
            
            // Queue all requests
            foreach (var url in urlList)
            {
                tasks.Add(GetAsync(url, priority, null, cancellationToken));
            }
            
            // Wait for all to complete
            await Task.WhenAll(tasks);
            
            // Return results with urls
            var results = new List<(string url, HttpResponseMessage response)>();
            for (int i = 0; i < urlList.Count; i++)
            {
                results.Add((urlList[i], tasks[i].Result));
            }
            
            return results;
        }
    }

    /// <summary>
    /// Configuration options for HttpClientUtility
    /// </summary>
    public class HttpClientUtilityOptions
    {
        public int MaxConcurrentRequests { get; set; } = 8;
        public int MaxRetryAttempts { get; set; } = 3;
        public int InitialRetryDelayMs { get; set; } = 1000;
        public int MaxRetryDelayMs { get; set; } = 15000;
        public int TimeoutSeconds { get; set; } = 30;
        public string AppVersion { get; set; } = "1.0.0";
        public bool EnableResponseCaching { get; set; } = true;
        public bool EnableHttp2 { get; set; } = true; // Add HTTP/2 support option
        public bool EnableCompression { get; set; } = true; // Add compression option
        public int CacheEntrySize { get; set; } = 100; // Used for memory calculation
    }
} 