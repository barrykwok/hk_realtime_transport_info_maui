using System;
using System.IO;
using System.Collections.Concurrent;
using hk_realtime_transport_info_maui.Models;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Threading.Tasks;
using NGeoHash;
using LiteDB;
using System.Reflection;

namespace hk_realtime_transport_info_maui.Services
{
    public class LiteDbService : IDisposable
    {
        private readonly string _dbPath;
        private readonly object _lock = new object();
        private LiteDatabase? _database;
        private readonly ILogger<LiteDbService>? _logger;
        
        // Cache for route stops to improve performance
        private readonly ConcurrentDictionary<string, List<TransportStop>> _routeStopsCache = new();
        
        // Cache of routes that have already had sequence corrections applied
        private readonly HashSet<string> _sequenceCorrectedRoutes = new();
        
        // Cache for routes by stop
        private ConcurrentDictionary<string, Tuple<DateTime, List<TransportRoute>>>? _routesByStopCache;
        
        // Add a private field for all stops caching
        private List<TransportStop> _cachedAllStops;
        
        public LiteDbService(ILogger<LiteDbService>? logger = null)
        {
            _dbPath = Path.Combine(FileSystem.AppDataDirectory, "transport_litedb.db");
            _logger = logger;
            
            try
            {
                // Ensure directory exists
                Directory.CreateDirectory(FileSystem.AppDataDirectory);
                
                // Initialize database
                var connectionString = $"Filename={_dbPath};Connection=shared";
                _database = new LiteDatabase(connectionString);
                
                _logger?.LogInformation("Creating database collections and indexes");
                    
                // Create indexes for each collection
                CreateIndexes();
                        
                _logger?.LogInformation("Database collections and indexes created successfully");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error initializing database");
                
                // Attempt to create database without optimizations as fallback
                try {
                    var connectionString = $"Filename={_dbPath};Connection=shared;Journal=false";
                    _database = new LiteDatabase(connectionString);
                        
                    _logger?.LogInformation("Created database with fallback approach");
                }
                catch (Exception fallbackEx) {
                    _logger?.LogError(fallbackEx, "Critical error initializing database with fallback approach");
                }
            }
        }
        
        private LiteDatabase GetDatabase()
        {
            if (_database == null)
            {
                lock (_lock)
                {
                    if (_database == null)
                    {
                        var connectionString = $"Filename={_dbPath};Connection=shared";
                        _database = new LiteDatabase(connectionString);
                    }
                }
            }
            return _database;
        }
        
        private void EnsureMinimalIndexes()
        {
            // Just ensure minimal indexes to get things running
            var db = GetDatabase();
            
            db.GetCollection<TransportRoute>().EnsureIndex(r => r.Id);
            db.GetCollection<TransportStop>().EnsureIndex(s => s.Id);
            db.GetCollection<TransportEta>().EnsureIndex(e => e.Id);
            db.GetCollection<FavoriteRoute>().EnsureIndex(f => f.Id);
            db.GetCollection<RouteStopRelation>().EnsureIndex(r => r.Id);
        }
        
        public void EnsureIndexes()
        {
            CreateIndexes();
        }
        
        public void InsertRecord<T>(string collectionName, T record) where T : class
        {
            try
            {
                var db = GetDatabase();
                var collection = db.GetCollection<T>(collectionName);
                collection.Insert(record);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"Error inserting record into {collectionName}");
                throw;
            }
        }
        
        public void BulkInsert<T>(string collectionName, IEnumerable<T> records, int batchSize = 500) where T : class
        {
            if (records == null || !records.Any())
                return;
                
            try
            {
                var db = GetDatabase();
                var collection = db.GetCollection<T>(collectionName);
                
                var recordsList = records.ToList();
                int successCount = 0;
                
                // Process in batches using optimized batch size
                int optimalBatchSize = DetermineOptimalBatchSize(recordsList.Count);
                
                // Begin a transaction for better performance (note: BeginTrans returns bool, not a transaction object)
                db.BeginTrans();
                
                try
                {
                // Process in batches for better performance
                    for (int i = 0; i < recordsList.Count; i += optimalBatchSize)
                {
                        var batch = recordsList.Skip(i).Take(optimalBatchSize).ToList();
                    
                    try
                    {
                            // Extract IDs for more efficient processing
                            // Check if we can get Id property through reflection (for BsonId fields)
                            var idProperty = typeof(T).GetProperty("Id");
                            
                            if (idProperty != null)
                            {
                                // Get all existing IDs in one database query
                                var batchIds = batch.Select(r => idProperty.GetValue(r)?.ToString()).Where(id => id != null).ToArray();
                                var existingIds = new HashSet<string>();
                                
                                // Only query for existing IDs if we have IDs to check
                                if (batchIds.Length > 0)
                                {
                                    // Find existing records individually (LiteDB doesn't support IN queries well in this version)
                                    foreach (var id in batchIds)
                                    {
                                        if (id != null)
                                        {
                                            var existingItem = collection.FindById(id);
                                            if (existingItem != null)
                                            {
                                                existingIds.Add(id);
                                            }
                                        }
                                    }
                                }
                                
                                // Separate new records from existing ones
                                var newRecords = new List<T>();
                                var existingRecords = new List<T>();
                                
                        foreach (var record in batch)
                                {
                                    var id = idProperty.GetValue(record)?.ToString();
                                    
                                    if (string.IsNullOrEmpty(id) || !existingIds.Contains(id))
                                    {
                                        newRecords.Add(record);
                                    }
                                    else
                                    {
                                        existingRecords.Add(record);
                                    }
                                }
                                
                                // Bulk insert new records
                                if (newRecords.Count > 0)
                        {
                            try
                            {
                                        collection.InsertBulk(newRecords);
                                        successCount += newRecords.Count;
                                        _logger?.LogDebug($"Bulk inserted {newRecords.Count} new records in {collectionName}");
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger?.LogError(ex, $"Error bulk inserting {newRecords.Count} new records in {collectionName}");
                                        
                                        // Fall back to individual inserts if bulk insert fails
                                        foreach (var record in newRecords)
                                        {
                                            try
                                            {
                                                collection.Insert(record);
                                    successCount++;
                                }
                                            catch (Exception insertEx)
                            {
                                                _logger?.LogDebug($"Could not insert record in {collectionName}: {insertEx.Message}");
                                            }
                                        }
                                    }
                                }
                                
                                // Update existing records
                                foreach (var record in existingRecords)
                                {
                                    try
                                    {
                                        collection.Update(record);
                                        successCount++;
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger?.LogDebug($"Could not update record in {collectionName}: {ex.Message}");
                                    }
                                }
                            }
                            else
                            {
                                // Fallback for types without Id property - use individual upserts
                                foreach (var record in batch)
                                {
                                    try
                                    {
                                        collection.Upsert(record);
                                        successCount++;
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger?.LogDebug($"Could not upsert record in {collectionName}: {ex.Message}");
                            }
                                }
                            }
                            
                            if (i + optimalBatchSize < recordsList.Count && (i + optimalBatchSize) % 2000 == 0)
                            {
                                // Commit transaction every 2000 records to prevent transaction from growing too large
                                db.Commit();
                                // Start a new transaction
                                db.BeginTrans();
                        }
                    }
                    catch (Exception innerEx)
                    {
                            _logger?.LogError(innerEx, $"Error in batch insert for {collectionName}, batch {i / optimalBatchSize + 1}");
                    }
                }
                    
                    // Commit all changes
                    db.Commit();
                
                _logger?.LogInformation($"Successfully inserted/updated {successCount} records in {collectionName}");
                
                // Special cases for specific types of records
                if (typeof(T) == typeof(TransportRoute))
                {
                    // Handle route stop relationships
                    SaveRouteStopRelationships(recordsList.Cast<TransportRoute>().ToList());
                }
                else if (typeof(T) == typeof(TransportStop))
                {
                    // Handle stop operator ID relationships
                    SaveStopOperatorIdRelationships(recordsList.Cast<TransportStop>().ToList());
                    }
                }
                catch (Exception ex)
                {
                    // If any error occurs, rollback the transaction
                    db.Rollback();
                    _logger?.LogError(ex, $"Error in transaction, rolled back all changes for {collectionName}");
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, $"Error in bulk insert for {collectionName}");
                throw;
            }
        }
        
        // Helper method to determine optimal batch size based on record count
        private int DetermineOptimalBatchSize(int recordCount)
        {
            // For small collections, use a smaller batch size to avoid memory issues
            if (recordCount < 500)
                return Math.Max(50, recordCount);
            
            // For medium collections, use a medium batch size
            if (recordCount < 2000)
                return 500;
            
            // For large collections, use a larger batch size
            if (recordCount < 5000)
                return 1000;
            
            // For very large collections, use an even larger batch size
            return 2000;
        }
            
        private void SaveRouteStopRelationships(List<TransportRoute> routes)
        {
            if (routes == null || !routes.Any())
                return;
                
            try
            {
                var db = GetDatabase();
                var routeStopRelationCollection = db.GetCollection<RouteStopRelation>("RouteStopRelations");
            
                // Begin a transaction for better performance
                db.BeginTrans();
                
                try
                {
                    // Process in smaller batches for better performance
                    const int batchSize = 300;
                    int routesProcessed = 0;
                    int relationsProcessed = 0;
                    
                    for (int i = 0; i < routes.Count; i += batchSize)
                    {
                        var routeBatch = routes.Skip(i).Take(batchSize).ToList();
                        
                        // For each route in the batch
                        foreach (var route in routeBatch)
                {
                            if (route.StopRelations == null || !route.StopRelations.Any())
                                continue;
                            
                        // Get existing relations for this route
                        var existingRelations = routeStopRelationCollection
                            .Find(Query.EQ("RouteId", route.Id))
                            .ToList();
                        
                        // Delete existing relations that are not in the new set
                        var existingIds = existingRelations.Select(r => r.Id).ToList();
                        var newIds = route.StopRelations.Where(r => r.Id != 0).Select(r => r.Id).ToList();
                        var toDelete = existingIds.Except(newIds).ToList();
                        
                        if (toDelete.Any())
                        {
                            foreach (var id in toDelete)
                            {
                                routeStopRelationCollection.Delete(id);
                            }
                        }
                        
                            // Process relations in batches
                            if (route.StopRelations.Count > 0)
                            {
                                // Separate new from existing relations
                                var newRelations = route.StopRelations.Where(r => r.Id == 0).ToList();
                                var existingRelationsToUpdate = route.StopRelations.Where(r => r.Id != 0).ToList();
                                
                                // Bulk insert new relations if any
                                if (newRelations.Any())
                                {
                                    routeStopRelationCollection.InsertBulk(newRelations);
                                    relationsProcessed += newRelations.Count;
                                }
                                
                                // Update existing relations
                                foreach (var relation in existingRelationsToUpdate)
                                {
                                routeStopRelationCollection.Update(relation);
                                    relationsProcessed++;
                            }
                        }
                            
                            routesProcessed++;
                        }
                        
                        // Commit transaction every batch to prevent it from growing too large
                        db.Commit();
                        db.BeginTrans();
                        
                        _logger?.LogDebug("Processed {processed}/{total} routes with their stop relations", 
                            routesProcessed, routes.Count);
                    }
                    
                    // Commit the final transaction
                    db.Commit();
                    
                    _logger?.LogInformation("Successfully processed {routes} routes with {relations} stop relations", 
                        routesProcessed, relationsProcessed);
                }
                catch (Exception ex)
                {
                    // If any error occurs, rollback the transaction
                    db.Rollback();
                    _logger?.LogError(ex, "Error in transaction, rolled back changes for route-stop relationships");
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error saving route-stop relationships");
            }
        }
        
        private void SaveStopOperatorIdRelationships(List<TransportStop> stops)
        {
            // This method would handle any special stop-related relationships for LiteDB
        }
        
        public IEnumerable<T> GetAllRecords<T>(string collectionName) where T : class, new()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<T>(collectionName);
            return collection.FindAll().ToList();
        }
        
        public IEnumerable<TransportRoute> GetRoutesWithoutStops()
        {
            var db = GetDatabase();
            var routeCollection = db.GetCollection<TransportRoute>("TransportRoutes");
            var relationCollection = db.GetCollection<RouteStopRelation>("RouteStopRelations");

            var allRoutes = routeCollection.FindAll().ToList();
            if (!allRoutes.Any())
            {
                _logger?.LogDebug("GetRoutesWithoutStops: No routes found in TransportRoutes collection.");
                return Enumerable.Empty<TransportRoute>();
            }

            // Get all unique RouteIds from RouteStopRelations.
            // This should be more efficient than FindAll().Select().Distinct() if an index on RouteId is used by LiteDB's Query().Select().
            _logger?.LogDebug("GetRoutesWithoutStops: Fetching distinct RouteIds from RouteStopRelations.");
            var routeIdsWithStops = new HashSet<string>(
                relationCollection.Query()
                                  .Select(x => x.RouteId) // Query only the RouteId field
                                  .ToEnumerable()        // Execute the query
                                  .Distinct()            // Get distinct IDs
            );
            _logger?.LogDebug("GetRoutesWithoutStops: Found {Count} distinct RouteIds in RouteStopRelations.", routeIdsWithStops.Count);

            if (!routeIdsWithStops.Any())
            {
                _logger?.LogDebug("GetRoutesWithoutStops: No route IDs found in RouteStopRelations. Returning all {Count} routes.", allRoutes.Count);
                return allRoutes; // No relations means all routes are "without stops" in this context
            }

            var routesWithoutStopsResult = allRoutes.Where(r => !routeIdsWithStops.Contains(r.Id)).ToList();
            _logger?.LogDebug("GetRoutesWithoutStops: Filtered down to {Count} routes without stops.", routesWithoutStopsResult.Count);
            
            return routesWithoutStopsResult;
        }
        
        public T GetRecordById<T>(string collectionName, string id) where T : class, new()
        {
            var db = GetDatabase();
            var collection = db.GetCollection<T>(collectionName);
            return collection.FindById(new BsonValue(id));
        }
        
        public bool UpdateRecord<T>(string collectionName, T record) where T : class
        {
            var db = GetDatabase();
            var collection = db.GetCollection<T>(collectionName);
            return collection.Update(record);
        }
        
        public async Task ClearEtasForRoute(string routeId)
        {
            await Task.Run(() =>
            {
                try
                {
                    var db = GetDatabase();
                    var etaCollection = db.GetCollection<TransportEta>("TransportEtas");
                
                    // Delete all ETAs for this route
                    etaCollection.DeleteMany(Query.EQ("RouteId", routeId));
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, $"Error clearing ETAs for route {routeId}");
                }
            });
        }
        
        public async Task SaveEtas(List<TransportEta> etas)
        {
            if (etas == null || !etas.Any())
                return;
            
            await Task.Run(() =>
            {
                try
                {
                    var db = GetDatabase();
                    var etaCollection = db.GetCollection<TransportEta>("TransportEtas");
                    
                    // Begin a transaction for better performance
                    db.BeginTrans();
                    
                    try
                    {
                    // Group ETAs by route and stop for more efficient processing
                    var etaGroups = etas.GroupBy(e => new { e.RouteId, e.StopId }).ToList();
                    
                    foreach (var group in etaGroups)
                    {
                        // Delete existing ETAs for this route+stop combination
                        etaCollection.DeleteMany(Query.And(
                            Query.EQ("RouteId", group.Key.RouteId),
                            Query.EQ("StopId", group.Key.StopId)
                        ));
                        
                            // Further deduplicate ETAs by keeping only the latest for each direction + sequence combination
                            var uniqueEtas = group
                                .GroupBy(e => new { e.Direction, Sequence = e.Id.Split('_').Length > 2 ? e.Id.Split('_')[2] : "0" })
                                .Select(g => g.OrderByDescending(e => e.FetchTime).First())
                                .ToList();
                            
                            _logger?.LogDebug("SaveEtas: Filtered from {originalCount} to {uniqueCount} ETAs for route {routeId}, stop {stopId}",
                                group.Count(), uniqueEtas.Count, group.Key.RouteId, group.Key.StopId);
                            
                            try
                            {
                                // Try bulk insert of all ETAs
                                etaCollection.InsertBulk(uniqueEtas);
                            }
                            catch (Exception ex) when (ex.Message.Contains("duplicate key"))
                            {
                                // If we encounter duplicate keys, try individual inserts with error handling
                                _logger?.LogWarning(ex, "Encountered duplicate keys during bulk ETA insert, falling back to individual inserts");
                                
                                // Insert ETAs one by one to avoid the duplicate key issue
                                foreach (var eta in uniqueEtas)
                                {
                                    try
                                    {
                                        // Insert the ETA record
                                        etaCollection.Insert(eta);
                                    }
                                    catch (Exception insertEx)
                                    {
                                        // Log but continue with other ETAs
                                        _logger?.LogDebug("Could not insert ETA: {message}", insertEx.Message);
                                    }
                                }
                            }
                    }
                    
                    // Cleanup old ETAs
                    var cutoffTime = DateTime.Now.AddMinutes(-20);
                    etaCollection.DeleteMany(Query.LT("EtaTime", cutoffTime));
                        
                        // Commit the transaction
                        db.Commit();
                    }
                    catch (Exception ex)
                    {
                        // Rollback the transaction on error
                        db.Rollback();
                        _logger?.LogError(ex, "Transaction error saving ETAs to database");
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error saving ETAs to database");
                }
            });
        }
        
        public List<TransportStop> GetSortedStopsForRoute(string routeId)
        {
            var db = GetDatabase();
            var stopCollection = db.GetCollection<TransportStop>("TransportStops");
            var relationCollection = db.GetCollection<RouteStopRelation>("RouteStopRelations");
            
            // Get the stop relations for this route, ordered by sequence
            var relations = relationCollection
                .Find(Query.EQ("RouteId", routeId))
                .OrderBy(r => r.Sequence)
                .ToList();
            
            if (!relations.Any())
                return new List<TransportStop>();
                
            // Now get the actual stops
            var stops = new List<TransportStop>();
            foreach (var relation in relations)
            {
                var stop = stopCollection.FindById(relation.StopId);
                if (stop != null)
                {
                    // Set the sequence number from the relation
                    stop.Sequence = relation.Sequence;
                    stops.Add(stop);
                }
            }
            
            return stops;
        }
        
        public void PrefetchStopsForRoutes(IEnumerable<string> routeIds)
        {
            if (routeIds == null || !routeIds.Any())
                return;
            
            try
            {
                foreach (var routeId in routeIds)
                {
                    if (!_routeStopsCache.ContainsKey(routeId))
                    {
                        var stops = GetStopsForRoute(routeId);
                        _routeStopsCache[routeId] = stops;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error prefetching stops for routes");
            }
        }
        
        public List<TransportStop> GetStopsForRoute(string routeId)
        {
            // Try to get from cache first
            if (_routeStopsCache.TryGetValue(routeId, out var cachedStops))
            {
                return cachedStops;
            }
            
            // Not in cache, so get from database and add to cache
            var stops = GetSortedStopsForRoute(routeId);
            _routeStopsCache[routeId] = stops;
            return stops;
        }
        
        public void ClearRouteStopsCache()
        {
            _routeStopsCache.Clear();
        }
        
        public Task<List<RouteStopRelation>> GetRouteStopRelationsForRoute(string routeId)
        {
            return Task.Run(() =>
            {
                var db = GetDatabase();
                var relationCollection = db.GetCollection<RouteStopRelation>("RouteStopRelations");
                
                return relationCollection
                    .Find(Query.EQ("RouteId", routeId))
                    .OrderBy(r => r.Sequence)
                    .ToList();
            });
        }
        
        public TransportRoute? GetRouteByNumberBoundAndServiceType(string routeNumber, string bound, string serviceType, TransportOperator transportOperator)
        {
            var db = GetDatabase();
            var routeCollection = db.GetCollection<TransportRoute>("TransportRoutes");
            
            return routeCollection.FindOne(Query.And(
                Query.EQ("RouteNumber", routeNumber),
                Query.EQ("Bound", bound),
                Query.EQ("ServiceType", serviceType),
                Query.EQ("Operator", (int)transportOperator)
            ));
        }
        
        public int CountRouteDirections(string routeNumber, string serviceType, TransportOperator transportOperator)
        {
            var db = GetDatabase();
            var routeCollection = db.GetCollection<TransportRoute>("TransportRoutes");
            
            return routeCollection.Count(Query.And(
                Query.EQ("RouteNumber", routeNumber),
                Query.EQ("ServiceType", serviceType),
                Query.EQ("Operator", (int)transportOperator)
            ));
        }
        
        public List<string> GetRouteDirections(string routeNumber, string serviceType, TransportOperator transportOperator)
        {
            var db = GetDatabase();
            var routeCollection = db.GetCollection<TransportRoute>("TransportRoutes");
            
            var routes = routeCollection.Find(Query.And(
                Query.EQ("RouteNumber", routeNumber),
                Query.EQ("ServiceType", serviceType),
                Query.EQ("Operator", (int)transportOperator)
            )).ToList();
            
            return routes.Select(r => r.Bound).Distinct().ToList();
        }
        
        public void CloseAndReopenConnection()
        {
            lock (_lock)
            {
                try
                {
                    // Close current connection
                    _database?.Dispose();
                    _database = null;
                    
                    // Force GC to clean up resources
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    
                    // Reopen with a delay to ensure resources are released
                    Task.Delay(100).Wait();
                    
                    // Reopen connection
                    var connectionString = $"Filename={_dbPath};Connection=shared";
                    _database = new LiteDatabase(connectionString);
                    
                    _logger?.LogInformation("Database connection closed and reopened successfully");
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error closing and reopening database connection");
                    
                    // Try fallback approach
                    try
                    {
                        _database?.Dispose();
                        _database = null;
                        
                        Task.Delay(500).Wait();
                        
                        var connectionString = $"Filename={_dbPath};Connection=shared;Journal=false";
                        _database = new LiteDatabase(connectionString);
                    
                        _logger?.LogInformation("Database connection reopened with fallback approach");
                    }
                    catch (Exception fallbackEx)
                    {
                        _logger?.LogError(fallbackEx, "Critical error reopening database connection");
                    }
                }
            }
        }
        
        public void Dispose()
        {
            lock (_lock)
            {
                if (_database != null)
                {
                    try
                    {
                        _database.Dispose();
                        _database = null;
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Error disposing database");
                    }
                }
            }
        }
        
        public async Task<List<TransportStop>> GetAllStopsAsync()
        {
            // Check if we have cached stops first
            if (_cachedAllStops != null)
            {
                _logger?.LogDebug("Returning {count} cached stops from memory", _cachedAllStops.Count);
                return _cachedAllStops;
            }
            
            return await Task.Run(() =>
            {
                try
            {
                var db = GetDatabase();
                var stopCollection = db.GetCollection<TransportStop>("TransportStops");
                
                    // Use a more efficient query approach with projection
                    var stops = stopCollection.FindAll().ToList();
                    
                    // Cache in memory for future use
                    _cachedAllStops = stops;
                    
                    _logger?.LogInformation("Loaded {count} stops from database", stops.Count);
                    return stops;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error loading all stops from database");
                    return new List<TransportStop>();
                }
            });
        }
        
        public async Task<List<TransportRoute>> GetRoutesForStopAsync(string stopId)
        {
            // Check cache first
            if (_routesByStopCache != null && 
                _routesByStopCache.TryGetValue(stopId, out var cachedResult) && 
                (DateTime.Now - cachedResult.Item1).TotalMinutes < 10)
            {
                return cachedResult.Item2;
            }
            
            return await Task.Run(() =>
            {
                var db = GetDatabase();
                var routeCollection = db.GetCollection<TransportRoute>("TransportRoutes");
                var relationCollection = db.GetCollection<RouteStopRelation>("RouteStopRelations");
                
                // Get all route IDs that include this stop
                var routeIds = relationCollection
                    .Find(Query.EQ("StopId", stopId))
                    .Select(r => r.RouteId)
                    .Distinct()
                    .ToList();
                
                if (!routeIds.Any())
                    return new List<TransportRoute>();
            
                // Get the actual routes
                var routes = new List<TransportRoute>();
                foreach (var routeId in routeIds)
                {
                    var route = routeCollection.FindById(routeId);
                    if (route != null)
                    {
                        routes.Add(route);
                    }
                }
                
                // Cache the result
                if (_routesByStopCache == null)
                {
                    _routesByStopCache = new ConcurrentDictionary<string, Tuple<DateTime, List<TransportRoute>>>();
                }
                
                _routesByStopCache[stopId] = new Tuple<DateTime, List<TransportRoute>>(DateTime.Now, routes);
                
                return routes;
            });
        }
        
        public bool CheckAndRepairDatabase()
        {
            try
            {
                var db = GetDatabase();
                
                // Check if collections exist, and create them if they don't
                EnsureCollections(db);
                    
                // Ensure indexes are created
                CreateIndexes();
                    
                // Run the database checkpoint to ensure consistency
                db.Checkpoint();
                    
                // Rebuild the database to optimize storage
                db.Rebuild();
                
                return true;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error checking and repairing database");
                
                // Try to rebuild the database as a last resort
                try
                {
                    // Close and dispose the current database
                    _database?.Dispose();
                    _database = null;
                        
                    // Wait for resources to be released
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    Task.Delay(500).Wait();
                        
                    // Create a backup of the current database file if it exists
                    if (File.Exists(_dbPath))
                    {
                        var backupPath = _dbPath + ".bak";
                        if (File.Exists(backupPath))
                        {
                            File.Delete(backupPath);
                        }
                        File.Copy(_dbPath, backupPath);
                    }
                        
                    // Create a new database
                    var connectionString = $"Filename={_dbPath};Connection=shared";
                    _database = new LiteDatabase(connectionString);
                        
                    // Create collections and indexes in the new database
                    EnsureCollections(_database);
                    CreateIndexes();
                    
                    return true;
                }
                catch (Exception rebuildEx)
                {
                    _logger?.LogError(rebuildEx, "Critical error rebuilding database");
                    return false;
                }
            }
        }
        
        private void EnsureCollections(LiteDatabase db)
        {
            if (!db.CollectionExists("TransportRoutes"))
                db.GetCollection<TransportRoute>("TransportRoutes");
                
            if (!db.CollectionExists("TransportStops"))
                db.GetCollection<TransportStop>("TransportStops");
                
            if (!db.CollectionExists("TransportEtas"))
                db.GetCollection<TransportEta>("TransportEtas");
                
            if (!db.CollectionExists("FavoriteRoutes"))
                db.GetCollection<FavoriteRoute>("FavoriteRoutes");
                
            if (!db.CollectionExists("Holidays"))
                db.GetCollection<Holiday>("Holidays");
                
            if (!db.CollectionExists("RouteStopRelations"))
                db.GetCollection<RouteStopRelation>("RouteStopRelations");
        }
        
        private void CreateIndexes()
        {
            var db = GetDatabase();
            
            // Create indexes for all collections
            
            // TransportRoutes
            var routeCollection = db.GetCollection<TransportRoute>("TransportRoutes");
            routeCollection.EnsureIndex(r => r.Id);
            routeCollection.EnsureIndex(r => r.RouteNumber);
            routeCollection.EnsureIndex(r => r.Operator);
            routeCollection.EnsureIndex(r => r.Key, unique: true);
            routeCollection.EnsureIndex(r => r.Type);
            routeCollection.EnsureIndex(r => r.ServiceType);
            routeCollection.EnsureIndex(r => r.Bound);
            
            // TransportStops
            var stopCollection = db.GetCollection<TransportStop>("TransportStops");
            stopCollection.EnsureIndex(s => s.Id);
            stopCollection.EnsureIndex(s => s.StopId);
            stopCollection.EnsureIndex(s => s.Operator);
            stopCollection.EnsureIndex(s => s.Key, unique: true);
            stopCollection.EnsureIndex(s => s.Latitude);
            stopCollection.EnsureIndex(s => s.Longitude);
            stopCollection.EnsureIndex(s => s.GeoHash6);
            stopCollection.EnsureIndex(s => s.GeoHash7);
            stopCollection.EnsureIndex(s => s.GeoHash8);
            
            // TransportEtas
            var etaCollection = db.GetCollection<TransportEta>("TransportEtas");
            etaCollection.EnsureIndex(e => e.Id);
            etaCollection.EnsureIndex(e => e.RouteId);
            etaCollection.EnsureIndex(e => e.StopId);
            etaCollection.EnsureIndex(e => e.EtaTime);
            
            // FavoriteRoutes
            var favoriteCollection = db.GetCollection<FavoriteRoute>("FavoriteRoutes");
            favoriteCollection.EnsureIndex(f => f.Id);
            favoriteCollection.EnsureIndex(f => f.RouteId);
            favoriteCollection.EnsureIndex(f => f.StopId);
            favoriteCollection.EnsureIndex(f => f.Order);
            
            // Holidays
            var holidayCollection = db.GetCollection<Holiday>("Holidays");
            holidayCollection.EnsureIndex(h => h.Id);
            holidayCollection.EnsureIndex(h => h.Date);
            
            // RouteStopRelations
            var relationCollection = db.GetCollection<RouteStopRelation>("RouteStopRelations");
            relationCollection.EnsureIndex(r => r.Id);
            relationCollection.EnsureIndex(r => r.RouteId);
            relationCollection.EnsureIndex(r => r.StopId);
            relationCollection.EnsureIndex("RouteStop", "$.RouteId + '_' + $.StopId");
        }
        
        public void AnalyzeAndOptimizeDatabase()
        {
            try
            {
                var db = GetDatabase();
                
                // Run checkpoint to ensure data is properly persisted
                db.Checkpoint();
                
                // Rebuild the database to optimize storage
                db.Rebuild();
                
                _logger?.LogInformation("Database analyzed and optimized successfully");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error analyzing and optimizing database");
            }
        }
        
        public List<TransportStop> FindStopsNearLocation(double latitude, double longitude, int radiusMeters = 1000)
        {
            try
            {
                // If we have all stops cached, use in-memory filtering which is much faster
                if (_cachedAllStops != null && _cachedAllStops.Count > 0)
                {
                    _logger?.LogDebug("Using in-memory stop filtering for location search");
                    
                    // Calculate geohashes for faster filtering
                    string geoHash6 = GeoHash.Encode(latitude, longitude, 6);
                    string geoHash5 = geoHash6.Substring(0, 5);
                    
                    // First use geohash prefix filtering (very fast)
                    var potentialMatches = _cachedAllStops
                        .Where(s => s.GeoHash6 == geoHash6 || 
                               s.GeoHash6.StartsWith(geoHash5) || 
                               geoHash6.StartsWith(s.GeoHash6.Substring(0, 5)))
                        .ToList();
                    
                    // Then do exact distance calculation on the smaller set
                    var nearbyStops = new List<TransportStop>();
                    foreach (var stop in potentialMatches)
                    {
                        double distance = CalculateDistanceInMeters(latitude, longitude, stop.Latitude, stop.Longitude);
                        if (distance <= radiusMeters)
                        {
                            // Add distance information to the stop
                            stop.DistanceFromUser = distance;
                            nearbyStops.Add(stop);
                        }
                    }
                    
                    // Sort by distance
                    return nearbyStops.OrderBy(s => s.DistanceFromUser).ToList();
                }
                
                // Fall back to database query if cache is not available
                var db = GetDatabase();
                var stopCollection = db.GetCollection<TransportStop>("TransportStops");
                
                // Step 1: Convert radius to degrees (approximate)
                // 1 degree of latitude = ~111,000 meters
                // 1 degree of longitude = ~111,000 meters * cos(latitude)
                double latDegrees = radiusMeters / 111000.0;
                double longDegrees = radiusMeters / (111000.0 * Math.Cos(latitude * Math.PI / 180));
                
                // Step 2: Create a bounding box
                double minLat = latitude - latDegrees;
                double maxLat = latitude + latDegrees;
                double minLong = longitude - longDegrees;
                double maxLong = longitude + longDegrees;
                
                // Step 3: Find stops within the bounding box
                var stops = stopCollection.Find(Query.And(
                    Query.GTE("Latitude", minLat),
                    Query.LTE("Latitude", maxLat),
                    Query.GTE("Longitude", minLong),
                    Query.LTE("Longitude", maxLong)
                )).ToList();
                
                // Step 4: Filter further to ensure they're within the radius
                var result = new List<TransportStop>();
                foreach (var stop in stops)
                {
                    double distance = CalculateDistanceInMeters(latitude, longitude, stop.Latitude, stop.Longitude);
                    if (distance <= radiusMeters)
                    {
                        stop.DistanceFromUser = distance;
                        result.Add(stop);
                    }
                }
                
                // Step 5: Sort by distance
                return result.OrderBy(s => s.DistanceFromUser).ToList();
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error finding stops near location");
                return new List<TransportStop>();
            }
        }
        
        private double CalculateDistanceInMeters(double lat1, double lon1, double lat2, double lon2)
        {
            // Calculate distance using the Haversine formula
            double R = 6371000; // Earth radius in meters
            double dLat = DegreesToRadians(lat2 - lat1);
            double dLon = DegreesToRadians(lon2 - lon1);
            
            double a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                    Math.Cos(DegreesToRadians(lat1)) * Math.Cos(DegreesToRadians(lat2)) *
                    Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
                    
            double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            return R * c;
        }
        
        private double DegreesToRadians(double degrees)
        {
            return degrees * Math.PI / 180;
        }
        
        // Add method to clear stop cache when needed
        public void ClearStopCache()
        {
            _cachedAllStops = null;
            _logger?.LogInformation("Cleared stop cache");
        }
    }
} 