using System;
using System.IO;
using SQLite;
using System.Collections.Concurrent;
using hk_realtime_transport_info_maui.Models;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Threading.Tasks;
using NGeoHash;

namespace hk_realtime_transport_info_maui.Services
{
    public class DatabaseService : IDisposable
    {
        private readonly string _dbPath;
        private readonly object _lock = new object();
        private SQLiteConnection? _connection;
        private readonly ILogger<DatabaseService>? _logger;
        
        // Cache for route stops to improve performance
        private readonly ConcurrentDictionary<string, List<TransportStop>> _routeStopsCache = new();
        
        // Cache of routes that have already had sequence corrections applied
        private readonly HashSet<string> _sequenceCorrectedRoutes = new();
        
        // SQLite open flags without WAL mode initially
        private const SQLiteOpenFlags OpenFlags = 
            SQLiteOpenFlags.ReadWrite | 
            SQLiteOpenFlags.Create;
        
        // Private class for query results that include sequence
        private class TransportStopWithSequence
        {
            public string Id { get; set; } = string.Empty;
            public string StopId { get; set; } = string.Empty;
            public TransportOperator Operator { get; set; }
            public string NameEn { get; set; } = string.Empty;
            public string NameZh { get; set; } = string.Empty;
            public string NameZhHant { get; set; } = string.Empty;
            public string NameZhHans { get; set; } = string.Empty;
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            public string GeoHash6 { get; set; } = string.Empty;
            public string GeoHash7 { get; set; } = string.Empty;
            public string GeoHash8 { get; set; } = string.Empty;
            public string GeoHash9 { get; set; } = string.Empty;
            public DateTime LastUpdated { get; set; }
            public int Sequence { get; set; }
            
            // Convert to TransportStop but keep the sequence in a temporary field
            public TransportStop ToTransportStop()
            {
                var stop = new TransportStop
                {
                    Id = Id,
                    StopId = StopId,
                    Operator = Operator,
                    NameEn = NameEn,
                    NameZh = NameZh,
                    NameZhHant = NameZhHant,
                    NameZhHans = NameZhHans,
                    Latitude = Latitude,
                    Longitude = Longitude,
                    GeoHash6 = GeoHash6,
                    GeoHash7 = GeoHash7,
                    GeoHash8 = GeoHash8,
                    GeoHash9 = GeoHash9,
                    LastUpdated = LastUpdated
                };
                
                // Use reflection to set the Sequence property if it exists (to avoid compile-time errors)
                var seqProperty = typeof(TransportStop).GetProperty("Sequence");
                if (seqProperty != null)
                {
                    seqProperty.SetValue(stop, Sequence);
                }
                
                return stop;
            }
        }
        
        // Cache for routes by stop
        private ConcurrentDictionary<string, Tuple<DateTime, List<TransportRoute>>>? _routesByStopCache;
        
        public DatabaseService(ILogger<DatabaseService>? logger = null)
        {
            _dbPath = Path.Combine(FileSystem.AppDataDirectory, "transport.db");
            _logger = logger;
            
            try
            {
                // Ensure directory exists
                Directory.CreateDirectory(FileSystem.AppDataDirectory);
                
                // Initialize database and create tables FIRST (before enabling WAL)
                using (var db = new SQLiteConnection(_dbPath, OpenFlags))
                {
                    _logger?.LogInformation("Creating database tables and indexes");
                    
                    // Create tables for all model types - SQLite automatically creates indexes for [PrimaryKey] and [Indexed] attributes
                    db.CreateTable<TransportRoute>();
                    db.CreateTable<TransportStop>();
                    db.CreateTable<TransportEta>();
                    db.CreateTable<FavoriteRoute>();
                    db.CreateTable<Holiday>();
                    db.CreateTable<RouteStopRelation>();
                    
                    // For backwards compatibility - we'll keep this table
                    db.Execute("CREATE TABLE IF NOT EXISTS RouteStops (" +
                               "Id TEXT PRIMARY KEY, " +
                               "RouteId TEXT NOT NULL, " +
                               "StopId TEXT NOT NULL, " +
                               "Sequence INTEGER NOT NULL)");
                    
                    db.Execute("CREATE TABLE IF NOT EXISTS StopOperatorIds (" +
                               "Id TEXT PRIMARY KEY, " +
                               "StopId TEXT NOT NULL, " +
                               "OperatorName TEXT NOT NULL, " +
                               "OperatorStopId TEXT NOT NULL)");
                    
                    // Create indexes on relationship tables
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_routestoprelation_routeid ON RouteStopRelations (RouteId)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_routestoprelation_stopid ON RouteStopRelations (StopId)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_stopoperatorids_stopid ON StopOperatorIds (StopId)");
                    
                    // Create any additional composite indexes that may be helpful
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_route_compound ON TransportRoute (RouteNumber, ServiceType, Bound)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_stop_location ON TransportStop (Latitude, Longitude)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_eta_route_stop ON TransportEta (RouteId, StopId)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_favorite_order ON FavoriteRoute ([Order])");
                    
                    // Enhanced indexes for better performance
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_eta_time ON TransportEta (EtaTime)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_eta_combined ON TransportEta (RouteId, StopId, EtaTime)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_route_number_operator ON TransportRoute (RouteNumber, Operator)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_stop_geohash ON TransportStop (GeoHash6, GeoHash7, GeoHash8)");
                    db.Execute("CREATE INDEX IF NOT EXISTS idx_routestoprelation_combined ON RouteStopRelations (RouteId, StopId, Sequence)");
                    
                    _logger?.LogInformation("Database tables and indexes created successfully");
                    
                    // NOW enable WAL mode and performance settings AFTER tables are created
                    _logger?.LogInformation("Configuring database performance settings");
                    
                    try {
                        // Enable WAL mode for better concurrency and performance
                        db.Execute("PRAGMA journal_mode=WAL;");
                        
                        // Additional performance optimizations
                        db.Execute("PRAGMA synchronous=NORMAL;");
                        db.Execute("PRAGMA temp_store=MEMORY;");
                        db.Execute("PRAGMA cache_size=-4000;");
                        db.Execute("PRAGMA mmap_size=268435456;");
                        
                        _logger?.LogInformation("Database performance settings configured successfully");
                    }
                    catch (Exception ex) {
                        // Ignore "not an error" exception which is actually a success status message
                        if (!ex.Message.Contains("not an error")) {
                            _logger?.LogWarning("Non-critical error configuring performance settings: {message}", ex.Message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error initializing database");
                
                // Attempt to create database without optimizations as fallback
                try {
                    using (var db = new SQLiteConnection(_dbPath, OpenFlags))
                    {
                        // Create tables as a fallback without performance settings
                        db.CreateTable<TransportRoute>();
                        db.CreateTable<TransportStop>();
                        db.CreateTable<TransportEta>();
                        db.CreateTable<FavoriteRoute>();
                        db.CreateTable<Holiday>();
                        db.CreateTable<RouteStopRelation>();
                        
                        _logger?.LogInformation("Created tables with fallback approach");
                    }
                }
                catch (Exception fallbackEx) {
                    _logger?.LogError(fallbackEx, "Critical error initializing database with fallback approach");
                }
            }
        }
        
        public SQLiteConnection GetDatabase()
        {
            // With WAL mode, we can reduce locking overhead for read operations
            // since multiple readers can access the database concurrently
            if (_connection == null)
            {
                lock (_lock) // Still need lock for initializing the connection
                {
                    if (_connection == null)
                    {
                        _connection = new SQLiteConnection(_dbPath, OpenFlags);
                        
                        // Ensure performance settings - but don't fail if they can't be applied
                        try {
                            // Ensure WAL mode is enabled
                            _connection.Execute("PRAGMA journal_mode=WAL;");
                            
                            // Performance optimizations
                            _connection.Execute("PRAGMA synchronous=NORMAL;");
                            _connection.Execute("PRAGMA temp_store=MEMORY;");
                            _connection.Execute("PRAGMA cache_size=-4000;");
                            _connection.Execute("PRAGMA mmap_size=268435456;");
                        }
                        catch (Exception ex) {
                            // Ignore "not an error" exception which is actually a success status message
                            if (!ex.Message.Contains("not an error")) {
                                _logger?.LogWarning("Non-critical error applying performance settings: {message}", ex.Message);
                            }
                        }
                    }
                }
            }
            
            return _connection;
        }
        
        /// <summary>
        /// Creates minimal required indexes during initialization
        /// </summary>
        private void EnsureMinimalIndexes()
        {
            // This method is kept for backward compatibility, but all indexes are now created during initialization
            _logger?.LogInformation("Indexes already created during database initialization");
        }
        
        /// <summary>
        /// Ensures all necessary indexes are created for improved search performance
        /// </summary>
        public void EnsureIndexes()
        {
            // This method is kept for backward compatibility, but all indexes are now created during initialization
            _logger?.LogInformation("Indexes already created during database initialization");
        }
        
        public void InsertRecord<T>(string collectionName, T record) where T : class
        {
            var db = GetDatabase();
            db.Insert(record);
        }
        
        /// <summary>
        /// Inserts multiple records in a single transaction for better performance
        /// </summary>
        public void BulkInsert<T>(string collectionName, IEnumerable<T> records, int batchSize = 500) where T : class
        {
            if (records == null || !records.Any())
                return;

            var db = GetDatabase();
            var recordsList = records.ToList();
            
            // Process in batches for better performance and memory usage
            for (int i = 0; i < recordsList.Count; i += batchSize)
            {
                var batch = recordsList.Skip(i).Take(batchSize).ToList();
                
                // Use a transaction for the entire batch
                db.BeginTransaction();
                try 
                {
                    // Simply use standard insert - already optimized in SQLite-net
                    foreach (var item in batch)
                    {
                        db.Insert(item);
                    }
                    db.Commit();
                }
                catch (Exception ex)
                {
                    db.Rollback();
                    _logger?.LogError(ex, "Error in bulk insert operation for collection {collectionName}", collectionName);
                    throw;
                }
            }
            
            // If dealing with TransportRoute or TransportStop, handle relationships
            if (records.FirstOrDefault() is TransportRoute)
            {
                SaveRouteStopRelationships(recordsList.Cast<TransportRoute>().ToList());
            }
            
            if (records.FirstOrDefault() is TransportStop)
            {
                SaveStopOperatorIdRelationships(recordsList.Cast<TransportStop>().ToList());
            }
        }
        
        private void SaveRouteStopRelationships(List<TransportRoute> routes)
        {
            var db = GetDatabase();
            
            foreach (var route in routes)
            {
                if (route.Stops?.Count > 0)
                {
                    db.BeginTransaction();
                    try 
                    {
                        // Delete existing relationships
                        db.Execute("DELETE FROM RouteStopRelations WHERE RouteId = ?", route.Id);
                        
                        // Insert new relationships using the RouteStopRelation model
                        for (int i = 0; i < route.Stops.Count; i++)
                        {
                            var stop = route.Stops[i];
                            var relation = new RouteStopRelation
                            {
                                RouteId = route.Id,
                                StopId = stop.Id,
                                Sequence = i + 1, // Start sequence from 1 instead of 0
                                Direction = route.Bound // Use bound as direction
                            };
                            
                            db.Insert(relation);
                        }
                        
                        // For backward compatibility, also update the legacy table
                        db.Execute("DELETE FROM RouteStops WHERE RouteId = ?", route.Id);
                        for (int i = 0; i < route.Stops.Count; i++)
                        {
                            var stop = route.Stops[i];
                            db.Execute("INSERT INTO RouteStops (Id, RouteId, StopId, Sequence) VALUES (?, ?, ?, ?)",
                                Guid.NewGuid().ToString(), route.Id, stop.Id, i + 1); // Start sequence from 1 instead of 0
                        }
                        
                        db.Commit();
                    }
                    catch (Exception ex)
                    {
                        db.Rollback();
                        _logger?.LogError(ex, "Error saving route-stop relationships for route {routeId}", route.Id);
                    }
                }
            }
        }
        
        private void SaveStopOperatorIdRelationships(List<TransportStop> stops)
        {
            // This method is kept for backward compatibility but is now a no-op
            // OperatorStopIds has been removed in favor of a direct Operator property
            _logger?.LogInformation("SaveStopOperatorIdRelationships is deprecated and does nothing");
        }
        
        public IEnumerable<T> GetAllRecords<T>(string collectionName) where T : class, new()
        {
            var db = GetDatabase();
            return db.Table<T>().ToList();
        }
        
        // Get routes without their stops for faster loading
        public IEnumerable<TransportRoute> GetRoutesWithoutStops()
        {
            try
            {
                var db = GetDatabase();
                
                // Use compiled query for better performance
                return db.Table<TransportRoute>().ToList();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in GetRoutesWithoutStops");
                return new List<TransportRoute>();
            }
        }
        
        public T GetRecordById<T>(string collectionName, string id) where T : class, new()
        {
            var db = GetDatabase();
            return db.Find<T>(id);
        }
        
        public bool UpdateRecord<T>(string collectionName, T record) where T : class
        {
            var db = GetDatabase();
            int rows = db.Update(record);
            return rows > 0;
        }
        
        /// <summary>
        /// Clears all ETA data for a specific route before inserting new data
        /// </summary>
        public async Task ClearEtasForRoute(string routeId)
        {
            try
            {
                var db = GetDatabase();
                
                // Use a transaction for better performance
                db.BeginTransaction();
                try
                {
                    // Delete all ETAs for this route
                    db.Execute("DELETE FROM TransportEta WHERE RouteId = ?", routeId);
                    db.Commit();
                    
                    _logger?.LogDebug("Cleared ETAs for route {routeId}", routeId);
                    
                    // Satisfy the async requirement
                    await Task.CompletedTask;
                }
                catch (Exception ex)
                {
                    db.Rollback();
                    _logger?.LogError(ex, "Error clearing ETAs for route {routeId}", routeId);
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in ClearEtasForRoute for {routeId}", routeId);
                throw;
            }
        }
        
        /// <summary>
        /// Saves ETA data to the database
        /// </summary>
        public async Task SaveEtas(List<TransportEta> etas)
        {
            if (etas == null || etas.Count == 0)
            {
                return;
            }
            
            try
            {
                var db = GetDatabase();
                
                // Create a set to track IDs we've seen to prevent duplicates
                HashSet<string> processedIds = new HashSet<string>();
                
                // Use a transaction for better performance
                db.BeginTransaction();
                try
                {
                    foreach (var eta in etas)
                    {
                        // Ensure the ETA has a unique ID
                        if (string.IsNullOrEmpty(eta.Id))
                        {
                            eta.Id = Guid.NewGuid().ToString();
                        }
                        
                        // Skip duplicate IDs in the same batch
                        if (processedIds.Contains(eta.Id))
                        {
                            _logger?.LogDebug("Skipping duplicate ETA ID {id}", eta.Id);
                            continue;
                        }
                        
                        // Add to processed set
                        processedIds.Add(eta.Id);
                        
                        // Use InsertOrReplace to handle existing records
                        db.InsertOrReplace(eta);
                    }
                    
                    db.Commit();
                    _logger?.LogDebug("Saved {count} ETAs to database", etas.Count);
                    
                    // Satisfy the async requirement
                    await Task.CompletedTask;
                }
                catch (Exception ex)
                {
                    db.Rollback();
                    _logger?.LogError(ex, "Error saving ETAs to database");
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in SaveEtas");
                throw;
            }
        }
        
        public List<TransportStop> GetSortedStopsForRoute(string routeId)
        {
            var db = GetDatabase();
            
            // Get the stops in the correct sequence using the RouteStopRelation model
            var query = "SELECT s.*, rs.Sequence FROM TransportStop s " +
                        "JOIN RouteStopRelations rs ON s.Id = rs.StopId " +
                        "WHERE rs.RouteId = ? " +
                        "ORDER BY rs.Sequence";
            
            // Use the custom class to capture the Sequence information
            var stopData = db.Query<TransportStopWithSequence>(query, routeId);
            
            // Convert to TransportStop objects
            var stops = stopData.Select(s => s.ToTransportStop()).ToList();
            
            // Check for zero-based sequences and correct if needed
            if (stops.Count > 0)
            {
                // Check if we've already corrected this route's sequences before
                bool alreadyCorrected = false;
                lock (_sequenceCorrectedRoutes)
                {
                    alreadyCorrected = _sequenceCorrectedRoutes.Contains(routeId);
                }
                
                if (!alreadyCorrected)
                {
                    var sequences = stopData.Select(s => s.Sequence).ToList();
                    
                    // Check if sequences start from 0
                    if (sequences.Count > 0 && sequences.Min() == 0)
                    {
                        _logger?.LogWarning("Sequence starts from 0 for route {routeId}, should start from 1", routeId);
                        
                        // Adjust sequence numbers if needed (increment all by 1)
                        for (int i = 0; i < stops.Count; i++)
                        {
                            var seqProperty = typeof(TransportStop).GetProperty("Sequence");
                            if (seqProperty != null)
                            {
                                var seqValue = seqProperty.GetValue(stops[i]);
                                if (seqValue != null)
                                {
                                    var currentSeq = (int)seqValue;
                                    seqProperty.SetValue(stops[i], currentSeq + 1);
                                }
                            }
                        }
                        
                        // Mark this route as corrected so we don't log warnings again
                        lock (_sequenceCorrectedRoutes)
                        {
                            _sequenceCorrectedRoutes.Add(routeId);
                        }
                    }
                }
            }
            
            return stops;
        }
        
        // This method is kept for backward compatibility but now does nothing
        private void LoadStopOperatorIds(TransportStop stop)
        {
            // OperatorStopIds has been removed - stop has a direct Operator property
        }
        
        public void PrefetchStopsForRoutes(IEnumerable<string> routeIds)
        {
            if (routeIds == null || !routeIds.Any())
                return;
                
            var routeIdList = routeIds.ToList();
            
            _logger?.LogInformation("Prefetching stops for {count} routes", routeIdList.Count);
            
            try
            {
                foreach (var routeId in routeIdList)
                {
                    // Only prefetch if not already in cache
                    if (!_routeStopsCache.ContainsKey(routeId))
                    {
                        var stops = GetSortedStopsForRoute(routeId);
                        _routeStopsCache[routeId] = stops;
                    }
                }
                
                _logger?.LogInformation("Prefetched stops for {count} routes", routeIdList.Count);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error prefetching stops for routes");
            }
        }
        
        public List<TransportStop> GetStopsForRoute(string routeId)
        {
            // Check cache first
            if (_routeStopsCache.TryGetValue(routeId, out var cachedStops))
            {
                return cachedStops;
            }
            
            // Not in cache, load from database
            var stops = GetSortedStopsForRoute(routeId);
            _routeStopsCache[routeId] = stops;
            return stops;
        }
        
        public void ClearRouteStopsCache()
        {
            _routeStopsCache.Clear();
            _sequenceCorrectedRoutes.Clear();
            _logger?.LogInformation("Route stops cache cleared");
        }
        
        /// <summary>
        /// Gets the RouteStopRelation objects for a route to map sequence numbers to stop IDs
        /// </summary>
        /// <param name="routeId">The route ID</param>
        /// <returns>A list of RouteStopRelation objects</returns>
        public Task<List<RouteStopRelation>> GetRouteStopRelationsForRoute(string routeId)
        {
            try
            {
                var db = GetDatabase();
                
                // Get all relations for this route
                var relations = db.Table<RouteStopRelation>()
                    .Where(r => r.RouteId == routeId)
                    .OrderBy(r => r.Sequence)
                    .ToList();
                
                _logger?.LogDebug("Found {count} route-stop relations for route {routeId}", relations.Count, routeId);
                
                return Task.FromResult(relations);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error getting route-stop relations for route {routeId}", routeId);
                return Task.FromResult(new List<RouteStopRelation>());
            }
        }
        
        public TransportRoute? GetRouteByNumberBoundAndServiceType(string routeNumber, string bound, string serviceType, TransportOperator transportOperator)
        {
            try
            {
                var db = GetDatabase();
                var query = "SELECT * FROM TransportRoute WHERE RouteNumber = ? AND Bound = ? AND ServiceType = ? AND Operator = ? LIMIT 1";
                return db.Query<TransportRoute>(query, routeNumber, bound, serviceType, (int)transportOperator).FirstOrDefault();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error getting route by number, bound and service type");
                return null;
            }
        }
        
        public int CountRouteDirections(string routeNumber, string serviceType, TransportOperator transportOperator)
        {
            try
            {
                var db = GetDatabase();
                var query = "SELECT COUNT(DISTINCT Bound) FROM TransportRoute WHERE RouteNumber = ? AND ServiceType = ? AND Operator = ?";
                return db.ExecuteScalar<int>(query, routeNumber, serviceType, (int)transportOperator);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error counting route directions");
                return 0;
            }
        }
        
        public List<string> GetRouteDirections(string routeNumber, string serviceType, TransportOperator transportOperator)
        {
            try
            {
                var db = GetDatabase();
                var query = "SELECT DISTINCT Bound FROM TransportRoute WHERE RouteNumber = ? AND ServiceType = ? AND Operator = ?";
                var results = db.Query<BoundResult>(query, routeNumber, serviceType, (int)transportOperator);
                return results.Select(r => r.Bound).ToList();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error getting route directions");
                return new List<string>();
            }
        }
        
        // Helper class for SQLite queries that return a single text column
        private class BoundResult
        {
            public string Bound { get; set; } = string.Empty;
        }
        
        /// <summary>
        /// Closes and reopens the database connection to recover from SQLite errors.
        /// </summary>
        public void CloseAndReopenConnection()
        {
            lock (_lock)
            {
                try
                {
                    _logger?.LogInformation("Closing and reopening database connection to recover from error");
                    
                    // Close existing connection if it exists
                    if (_connection != null)
                    {
                        try 
                        {
                            _connection.Close();
                            _connection.Dispose();
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogWarning(ex, "Error closing existing database connection during recovery");
                        }
                        finally
                        {
                            _connection = null;
                        }
                    }
                    
                    // Clear caches
                    ClearRouteStopsCache();
                    
                    // Force GC collection to release any lingering references
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    
                    // Reopen connection
                    _connection = new SQLiteConnection(_dbPath, OpenFlags);
                    
                    // Re-apply performance settings
                    try
                    {
                        _connection.Execute("PRAGMA journal_mode=WAL;");
                        _connection.Execute("PRAGMA synchronous=NORMAL;");
                        _connection.Execute("PRAGMA temp_store=MEMORY;");
                        _connection.Execute("PRAGMA cache_size=-4000;");
                        _connection.Execute("PRAGMA mmap_size=268435456;");
                    }
                    catch (Exception ex)
                    {
                        // Ignore "not an error" exception which is actually a success status message
                        if (!ex.Message.Contains("not an error"))
                        {
                            _logger?.LogWarning("Non-critical error applying performance settings during recovery: {message}", ex.Message);
                        }
                    }
                    
                    _logger?.LogInformation("Database connection successfully reopened");
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to reopen database connection during recovery");
                    throw; // Re-throw as this is a critical error
                }
            }
        }
        
        public void Dispose()
        {
            _connection?.Close();
            _connection?.Dispose();
        }
        
        /// <summary>
        /// Gets all stops from the database asynchronously
        /// </summary>
        /// <returns>List of all transport stops</returns>
        public async Task<List<TransportStop>> GetAllStopsAsync()
        {
            try
            {
                return await Task.Run(() => {
                    var db = GetDatabase();
                    
                    // Optimize the query by specifying a more efficient transaction mode
                    db.Execute("PRAGMA temp_store=MEMORY;");
                    
                    try
                    {
                        // Try to get all stops at once first (most efficient if it works)
                        return db.Table<TransportStop>().ToList();
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning("Error getting all stops at once, trying with batched loading: {message}", ex.Message);
                        
                        // Fallback to batched loading if above fails
                        List<TransportStop> allStops = new List<TransportStop>();
                        
                        // Get count first to know how many batches we need
                        int totalCount = db.ExecuteScalar<int>("SELECT COUNT(*) FROM TransportStop");
                        int batchSize = 500;
                        int offset = 0;
                        
                        while (offset < totalCount)
                        {
                            var batchStops = db.Query<TransportStop>(
                                "SELECT * FROM TransportStop LIMIT ? OFFSET ?", 
                                batchSize, offset);
                                
                            if (batchStops != null && batchStops.Count > 0)
                            {
                                allStops.AddRange(batchStops);
                                offset += batchSize;
                            }
                            else
                            {
                                // No more stops, or batch was empty
                                break;
                            }
                        }
                        
                        return allStops;
                    }
                });
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Critical error retrieving stops from database");
                
                // Return empty list as last resort to prevent app crash
                return new List<TransportStop>();
            }
        }
        
        /// <summary>
        /// Gets all routes that serve a specific stop
        /// </summary>
        /// <param name="stopId">The ID of the stop</param>
        /// <returns>List of routes that serve the specified stop</returns>
        public async Task<List<TransportRoute>> GetRoutesForStopAsync(string stopId)
        {
            try
            {
                // Cache key for this stop's routes
                string cacheKey = $"routes_for_stop_{stopId}";
            
                // Check if we have a memory cache for the routes
                if (_routesByStopCache == null)
                {
                    _routesByStopCache = new ConcurrentDictionary<string, Tuple<DateTime, List<TransportRoute>>>();
                }
            
                // Check if we have a cached result that's less than 10 minutes old
                if (_routesByStopCache.TryGetValue(cacheKey, out var cachedRoutes) && 
                    (DateTime.UtcNow - cachedRoutes.Item1).TotalMinutes < 10)
                {
                    return cachedRoutes.Item2;
                }
            
                var db = GetDatabase();
            
                // Use a more optimized query with a JOIN to get routes for this stop directly
                var query = @"
                    SELECT r.*
                    FROM TransportRoute r
                    INNER JOIN RouteStopRelations rs ON r.Id = rs.RouteId
                    WHERE rs.StopId = ?
                    ORDER BY r.RouteNumber";
            
                var routes = await Task.Run(() => 
                    db.Query<TransportRoute>(query, stopId));
            
                var routesList = routes.ToList();
            
                // Cache the result with a timestamp
                _routesByStopCache[cacheKey] = new Tuple<DateTime, List<TransportRoute>>(DateTime.UtcNow, routesList);
            
                return routesList;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error getting routes for stop {stopId}", stopId);
                return new List<TransportRoute>();
            }
        }
        
        /// <summary>
        /// Checks database for corruption and attempts to repair if possible
        /// </summary>
        /// <returns>True if database is healthy or was successfully repaired, false otherwise</returns>
        public bool CheckAndRepairDatabase()
        {
            _logger?.LogInformation("Checking database for corruption");
            
            try
            {
                // First try to close existing connections
                CloseAndReopenConnection();
                
                // Suggest garbage collection to free memory
                GC.Collect();
                GC.WaitForPendingFinalizers();
                
                // Test if database is accessible
                bool databaseAccessible = false;
                try
                {
                    using (var testConn = new SQLiteConnection(_dbPath, SQLiteOpenFlags.ReadWrite))
                    {
                        var result = testConn.ExecuteScalar<int>("SELECT 1");
                        databaseAccessible = (result == 1);
                    }
                }
                catch (SQLiteException ex) when (ex.Message.Contains("malformed") || ex.Message.Contains("corrupt"))
                {
                    _logger?.LogError(ex, "Database corruption confirmed during test");
                    databaseAccessible = false;
                }
                
                if (databaseAccessible)
                {
                    _logger?.LogInformation("Database appears to be healthy");
                    return true;
                }
                
                _logger?.LogWarning("Database corruption detected, attempting repair");
                
                // Try to backup the database first
                try
                {
                    string backupPath = _dbPath + ".backup";
                    if (File.Exists(_dbPath))
                    {
                        File.Copy(_dbPath, backupPath, true);
                        _logger?.LogInformation("Created database backup at {path}", backupPath);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to create database backup");
                }
                
                // Force all connections to close
                _connection = null;
                GC.Collect();
                GC.WaitForPendingFinalizers();
                
                // Attempt recovery by creating a new connection
                // This relies on SQLite's auto-recovery features
                try
                {
                    // First try with readonly mode
                    using (var recoveryConn = new SQLiteConnection(_dbPath, SQLiteOpenFlags.ReadOnly))
                    {
                        var result = recoveryConn.ExecuteScalar<int>("PRAGMA integrity_check");
                        _logger?.LogInformation("Database integrity check result: {result}", result);
                    }
                    
                    // Then reopen with write access
                    _connection = new SQLiteConnection(_dbPath, SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create);
                    
                    // Test new connection
                    _connection.Execute("PRAGMA journal_mode=WAL");
                    _connection.Execute("PRAGMA synchronous=NORMAL");
                    _connection.Execute("VACUUM");
                    
                    // CRITICAL: Verify database by executing a query
                    var testResult = _connection.ExecuteScalar<int>("SELECT COUNT(*) FROM sqlite_master");
                    
                    _logger?.LogInformation("Database repaired successfully");
                    return true;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Failed to recover database with first method");
                    
                    // If first recovery method fails, try creating a new database
                    try
                    {
                        _logger?.LogWarning("Attempting more aggressive recovery by recreating database");
                        
                        // Close and remove connection
                        _connection = null;
                        GC.Collect();
                        GC.WaitForPendingFinalizers();
                        
                        // If database exists but is corrupted beyond repair, recreate it
                        if (File.Exists(_dbPath))
                        {
                            // Create emergency backup with timestamp
                            string timestamp = DateTime.Now.ToString("yyyyMMddHHmmss");
                            string emergencyBackup = _dbPath + "." + timestamp + ".corrupt";
                            try
                            {
                                File.Copy(_dbPath, emergencyBackup, true);
                                _logger?.LogInformation("Created emergency backup at {path}", emergencyBackup);
                            }
                            catch (Exception backupEx)
                            {
                                _logger?.LogError(backupEx, "Failed to create emergency backup");
                            }
                            
                            // Delete corrupted database
                            File.Delete(_dbPath);
                            _logger?.LogInformation("Deleted corrupted database file");
                        }
                        
                        // Create a new database
                        _connection = new SQLiteConnection(_dbPath, SQLiteOpenFlags.ReadWrite | SQLiteOpenFlags.Create);
                        CreateTablesAndIndexes();
                        
                        _logger?.LogInformation("Created new database successfully");
                        return true;
                    }
                    catch (Exception recreateEx)
                    {
                        _logger?.LogError(recreateEx, "Failed to recreate database");
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error checking/repairing database");
                return false;
            }
        }
        
        /// <summary>
        /// Creates database tables and indexes
        /// </summary>
        private void CreateTablesAndIndexes()
        {
            _logger?.LogInformation("Creating database tables and indexes");
            
            var db = GetDatabase();
            
            // Create tables for all model types
            db.CreateTable<TransportRoute>();
            db.CreateTable<TransportStop>();
            db.CreateTable<TransportEta>();
            db.CreateTable<FavoriteRoute>();
            db.CreateTable<Holiday>();
            db.CreateTable<RouteStopRelation>();
            
            // Recreate RouteStops table for backwards compatibility
            db.Execute("CREATE TABLE IF NOT EXISTS RouteStops (" +
                       "Id TEXT PRIMARY KEY, " +
                       "RouteId TEXT NOT NULL, " +
                       "StopId TEXT NOT NULL, " +
                       "Sequence INTEGER NOT NULL)");
            
            db.Execute("CREATE TABLE IF NOT EXISTS StopOperatorIds (" +
                       "Id TEXT PRIMARY KEY, " +
                       "StopId TEXT NOT NULL, " +
                       "OperatorName TEXT NOT NULL, " +
                       "OperatorStopId TEXT NOT NULL)");
            
            // Create indexes
            db.Execute("CREATE INDEX IF NOT EXISTS idx_routestoprelation_routeid ON RouteStopRelations (RouteId)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_routestoprelation_stopid ON RouteStopRelations (StopId)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_stopoperatorids_stopid ON StopOperatorIds (StopId)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_route_compound ON TransportRoute (RouteNumber, ServiceType, Bound)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_stop_location ON TransportStop (Latitude, Longitude)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_eta_route_stop ON TransportEta (RouteId, StopId)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_favorite_order ON FavoriteRoute ([Order])");
            
            // Enhanced indexes for better performance
            db.Execute("CREATE INDEX IF NOT EXISTS idx_eta_time ON TransportEta (EtaTime)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_eta_combined ON TransportEta (RouteId, StopId, EtaTime)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_route_number_operator ON TransportRoute (RouteNumber, Operator)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_stop_geohash ON TransportStop (GeoHash6, GeoHash7, GeoHash8)");
            db.Execute("CREATE INDEX IF NOT EXISTS idx_routestoprelation_combined ON RouteStopRelations (RouteId, StopId, Sequence)");
            
            _logger?.LogInformation("Database tables and indexes created successfully");
        }
        
        /// <summary>
        /// Analyzes database performance and optimizes it
        /// </summary>
        public void AnalyzeAndOptimizeDatabase()
        {
            try
            {
                _logger?.LogInformation("Analyzing and optimizing database");
                var db = GetDatabase();
                
                // Analyze the database for optimization opportunities
                db.Execute("ANALYZE;");
                
                // Run optimization commands
                db.Execute("PRAGMA optimize;");
                
                // Run vacuum to compact the database
                int maxRetries = 3;
                int delayMilliseconds = 1000;
                for (int i = 0; i < maxRetries; i++)
                {
                    try
                    {
                        db.Execute("VACUUM;");
                        _logger?.LogInformation("VACUUM command successful.");
                        break; // Success, exit loop
                    }
                    catch (SQLite.SQLiteException ex) when (ex.Message.Contains("SQL logic error or missing database") || ex.Message.Contains("database is locked") || ex.Message.Contains("SQL statements in progress"))
                    {
                        _logger?.LogWarning(ex, $"Error executing VACUUM (attempt {i + 1}/{maxRetries}): {ex.Message}. Retrying in {delayMilliseconds}ms...");
                        if (i < maxRetries - 1)
                        {
                            System.Threading.Thread.Sleep(delayMilliseconds);
                            delayMilliseconds *= 2; // Exponential backoff
                        }
                        else
                        {
                            _logger?.LogError(ex, "VACUUM command failed after multiple retries.");
                            // Optionally rethrow or handle the persistent failure
                        }
                    }
                    catch (Exception ex)
                    {
                        // Catch other unexpected exceptions during VACUUM
                        _logger?.LogError(ex, $"Unexpected error during VACUUM (attempt {i + 1}/{maxRetries}): {ex.Message}");
                        if (i < maxRetries - 1)
                        {
                             System.Threading.Thread.Sleep(delayMilliseconds);
                             delayMilliseconds *= 2; // Exponential backoff
                        }
                        else
                        {
                             _logger?.LogError(ex, "VACUUM command failed after multiple retries due to unexpected error.");
                        }
                    }
                }
                
                // Update statistics for query planner
                db.Execute("ANALYZE sqlite_master;");
                
                _logger?.LogInformation("Database analysis and optimization completed");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error while analyzing and optimizing database");
            }
        }
        
        /// <summary>
        /// Analyzes the execution plan for a specific query (for debugging performance)
        /// </summary>
        public string AnalyzeQueryPlan(string query, params object[] parameters)
        {
            try
            {
                var db = GetDatabase();
                
                // Use EXPLAIN QUERY PLAN to get the execution plan
                var explainQuery = $"EXPLAIN QUERY PLAN {query}";
                var result = db.Query<QueryPlan>(explainQuery, parameters);
                
                // Format the results
                var planDetails = string.Join("\n", result.Select(r => 
                    $"Step {r.Id}: {r.Detail} (Parent: {r.Parent}, Order: {r.OrderBy})"));
                
                _logger?.LogDebug("Query plan for '{query}': \n{plan}", query, planDetails);
                
                return planDetails;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error analyzing query plan for '{query}'", query);
                return $"Error: {ex.Message}";
            }
        }
        
        // Helper class for query plan results
        private class QueryPlan
        {
            [Column("id")]
            public int Id { get; set; }
            
            [Column("parent")]
            public int Parent { get; set; }
            
            [Column("notused")]
            public int NotUsed { get; set; }
            
            [Column("detail")]
            public string Detail { get; set; } = string.Empty;
            
            public string OrderBy => NotUsed.ToString();
        }
        
        /// <summary>
        /// Finds stops near a location using efficient geohash-based spatial search
        /// </summary>
        /// <param name="latitude">Latitude of the point</param>
        /// <param name="longitude">Longitude of the point</param>
        /// <param name="radiusMeters">Search radius in meters (approximate)</param>
        /// <returns>List of stops near the specified location</returns>
        public List<TransportStop> FindStopsNearLocation(double latitude, double longitude, int radiusMeters = 1000)
        {
            try
            {
                // Generate geohash with different precision levels for the input location
                string geoHash6 = NGeoHash.GeoHash.Encode(latitude, longitude, 6);
                string geoHash7 = NGeoHash.GeoHash.Encode(latitude, longitude, 7);
                string geoHash8 = NGeoHash.GeoHash.Encode(latitude, longitude, 8);
                
                _logger?.LogDebug("Searching for stops near location with geohash: {gh6}, {gh7}, {gh8}", 
                    geoHash6, geoHash7, geoHash8);
                
                var db = GetDatabase();
                List<TransportStop> stopsNearby = new List<TransportStop>();
                
                // Strategy: First search with higher precision (geohash8), then expand if needed
                // Use the new geohash index for fast searching with prefix matching
                
                // First try with precision 8 (highest precision, smallest area)
                // Match first 4 characters (very close proximity)
                if (geoHash8.Length >= 4)
                {
                    string prefix8 = geoHash8.Substring(0, 4);
                    var query8 = "SELECT * FROM TransportStop WHERE GeoHash8 LIKE ? || '%'";
                    var stops8 = db.Query<TransportStop>(query8, prefix8).ToList();
                    
                    // If we have enough stops with precision 8, use them
                    if (stops8.Count >= 5)
                    {
                        _logger?.LogDebug("Found {count} stops with geohash8 prefix {prefix}", stops8.Count, prefix8);
                        stopsNearby.AddRange(stops8);
                    }
                }
                
                // If not enough stops from precision 8, try with precision 7
                if (stopsNearby.Count < 5 && geoHash7.Length >= 3)
                {
                    string prefix7 = geoHash7.Substring(0, 3);
                    var query7 = "SELECT * FROM TransportStop WHERE GeoHash7 LIKE ? || '%'";
                    var stops7 = db.Query<TransportStop>(query7, prefix7).ToList();
                    
                    if (stops7.Count >= 5)
                    {
                        _logger?.LogDebug("Found {count} stops with geohash7 prefix {prefix}", stops7.Count, prefix7);
                        // Only add stops that aren't already in the list
                        foreach (var stop in stops7)
                        {
                            if (!stopsNearby.Any(s => s.Id == stop.Id))
                            {
                                stopsNearby.Add(stop);
                            }
                        }
                    }
                }
                
                // Fall back to precision 6 if still not enough stops
                if (stopsNearby.Count < 5 && geoHash6.Length >= 2)
                {
                    string prefix6 = geoHash6.Substring(0, 2);
                    var query6 = "SELECT * FROM TransportStop WHERE GeoHash6 LIKE ? || '%'";
                    var stops6 = db.Query<TransportStop>(query6, prefix6).ToList();
                    
                    _logger?.LogDebug("Found {count} stops with geohash6 prefix {prefix}", stops6.Count, prefix6);
                    
                    // Only add stops that aren't already in the list
                    foreach (var stop in stops6)
                    {
                        if (!stopsNearby.Any(s => s.Id == stop.Id))
                        {
                            stopsNearby.Add(stop);
                        }
                    }
                }
                
                // Filter by actual distance (geohash is approximate)
                if (stopsNearby.Count > 0)
                {
                    var filteredStops = stopsNearby
                        .Where(stop => CalculateDistanceInMeters(latitude, longitude, stop.Latitude, stop.Longitude) <= radiusMeters)
                        .OrderBy(stop => CalculateDistanceInMeters(latitude, longitude, stop.Latitude, stop.Longitude))
                        .ToList();
                    
                    _logger?.LogDebug("Filtered to {count} stops within {radius}m", filteredStops.Count, radiusMeters);
                    return filteredStops;
                }
                
                return new List<TransportStop>();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error finding stops near location");
                return new List<TransportStop>();
            }
        }
        
        /// <summary>
        /// Calculates the approximate distance between two coordinates in meters using Haversine formula
        /// </summary>
        private double CalculateDistanceInMeters(double lat1, double lon1, double lat2, double lon2)
        {
            const double EarthRadiusKm = 6371.0;
            
            var dLat = DegreesToRadians(lat2 - lat1);
            var dLon = DegreesToRadians(lon2 - lon1);
            
            var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                    Math.Cos(DegreesToRadians(lat1)) * Math.Cos(DegreesToRadians(lat2)) *
                    Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
                    
            var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            return EarthRadiusKm * c * 1000; // Convert to meters
        }
        
        private double DegreesToRadians(double degrees)
        {
            return degrees * Math.PI / 180.0;
        }
    }
} 