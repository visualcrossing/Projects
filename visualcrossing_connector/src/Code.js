
/**
 * Visual Crossing Weather Data Connector for Looker Studio
 * 
 * This connector enables you to import weather data from Visual Crossing
 * into Looker Studio for analysis and visualization.
 * 
 * Features:
 * - Historical weather data and forecasts
 * - Daily or hourly granularity
 * - Single location or multiple store locations via Google Sheets
 * - Metric or US units
 * 
 * @author Visual Crossing Corporation
 * @version 1.0.0
 */

// ===== Utilities =====
var VC_BASE = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services';
var CACHE_MIN = 5; // cache minutes for API responses (reduced for shared reports)
var MAX_CACHE_SIZE = 90000; // 90KB to leave some buffer (100KB limit)
var CHUNK_SIZE = 80000; // 80KB per chunk to be safe
var cc = DataStudioApp.createCommunityConnector();

function log_(message, data) { 
  if (data) {
    console.log('[VisualCrossing] ' + message + ': ' + JSON.stringify(data));
  } else {
    console.log('[VisualCrossing] ' + message);
  }
}

function getCache() { return CacheService.getScriptCache(); }
function cacheGet(key) { return getCache().get(key); }
function cachePut(key, val, sec) { getCache().put(key, val, sec || CACHE_MIN*60); }

// ===== Chunked Cache Management =====
function cachePutChunked(key, data, sec) {
  var jsonString = JSON.stringify(data);
  var cache = getCache();
  var expiration = sec || CACHE_MIN*60;
  
  // If data is small enough, store normally
  if (jsonString.length <= MAX_CACHE_SIZE) {
    cache.put(key, jsonString, expiration);
    log_('Data cached normally', { key: key, size: jsonString.length });
    return { success: true, chunks: 1 };
  }
  
  // Split into chunks
  var chunks = [];
  var chunkIndex = 0;
  var start = 0;
  
  while (start < jsonString.length) {
    var end = Math.min(start + CHUNK_SIZE, jsonString.length);
    var chunk = jsonString.substring(start, end);
    var chunkKey = key + '_chunk_' + chunkIndex;
    
    chunks.push(chunkKey);
    cache.put(chunkKey, chunk, expiration);
    
    start = end;
    chunkIndex++;
  }
  
  // Store metadata about chunks
  var metadata = {
    chunks: chunks,
    totalSize: jsonString.length,
    chunkCount: chunks.length
  };
  
  cache.put(key + '_meta', JSON.stringify(metadata), expiration);
  log_('Data cached in chunks', { 
    key: key, 
    totalSize: jsonString.length, 
    chunks: chunks.length 
  });
  
  return { success: true, chunks: chunks.length };
}

function cacheGetChunked(key) {
  var cache = getCache();
  
  // First try to get as single item
  var singleData = cache.get(key);
  if (singleData) {
    log_('Data retrieved as single item', { key: key, size: singleData.length });
    return JSON.parse(singleData);
  }
  
  // Try to get metadata for chunked data
  var metadataStr = cache.get(key + '_meta');
  if (!metadataStr) {
    log_('No cached data found', { key: key });
    return null;
  }
  
  var metadata = JSON.parse(metadataStr);
  var chunks = metadata.chunks;
  
  // Retrieve all chunks
  var allChunks = [];
  var missingChunks = [];
  
  for (var i = 0; i < chunks.length; i++) {
    var chunkData = cache.get(chunks[i]);
    if (chunkData) {
      allChunks.push(chunkData);
    } else {
      missingChunks.push(chunks[i]);
    }
  }
  
  if (missingChunks.length > 0) {
    log_('Some chunks missing', { 
      key: key, 
      missing: missingChunks.length, 
      total: chunks.length 
    });
    return null;
  }
  
  // Reconstruct original data
  var reconstructedData = allChunks.join('');
  log_('Data reconstructed from chunks', { 
    key: key, 
    totalSize: reconstructedData.length, 
    chunks: chunks.length 
  });
  
  return JSON.parse(reconstructedData);
}

function cacheRemoveChunked(key) {
  var cache = getCache();
  
  // Try to remove as single item first
  cache.remove(key);
  
  // Try to remove chunked data
  var metadataStr = cache.get(key + '_meta');
  if (metadataStr) {
    var metadata = JSON.parse(metadataStr);
    var chunks = metadata.chunks;
    
    // Remove all chunks
    for (var i = 0; i < chunks.length; i++) {
      cache.remove(chunks[i]);
    }
    
    // Remove metadata
    cache.remove(key + '_meta');
    
    log_('Chunked cache removed', { key: key, chunks: chunks.length });
  }
}

// ===== Auth =====
function getAuthType() {
  var AuthTypes = cc.AuthType;
  return cc
    .newAuthTypeResponse()
    .setAuthType(AuthTypes.NONE)
    .build();
}
// No authentication functions needed for AuthTypes.NONE

// ===== Config UI =====
function getConfig(request) {
  var config = cc.getConfig();

  config.newTextInput()
    .setId('api_key')
    .setName('Visual Crossing API Key')
    .setHelpText('Create or find your key in Visual Crossing.')
    .setPlaceholder('YOUR_API_KEY');

  config.newSelectSingle()
    .setId('data_type')
    .setName('Data Granularity')
    .setHelpText('Daily (date grain) or Hourly (hour grain)')
    .addOption(config.newOptionBuilder().setLabel('Daily').setValue('daily'))
    .addOption(config.newOptionBuilder().setLabel('Hourly').setValue('hourly'))
    .setAllowOverride(true);

  config.newSelectSingle()
    .setId('units')
    .setName('Units')
    .addOption(config.newOptionBuilder().setLabel('US (°F, mph)').setValue('us'))
    .addOption(config.newOptionBuilder().setLabel('Metric (°C, m/s)').setValue('metric'))
    .setAllowOverride(true);

  config.newSelectSingle()
    .setId('source_mode')
    .setName('Source Mode')
    .setHelpText('Single location OR "Store Join (Sheet)"')
    .addOption(config.newOptionBuilder().setLabel('Single Location').setValue('single'))
    .addOption(config.newOptionBuilder().setLabel('Store Join (Sheet)').setValue('sheet'))
    .setAllowOverride(false);
    //.setIsDynamic(false)

  // Info section for single location
  config.newInfo()
    .setId('single_mode_info')
    .setText('📍 Single Location Mode: Enter one location below');

  // Single location fields
  config.newTextInput()
    .setId('location')
    .setName('Location (address or lat,lon)')
    .setHelpText('Examples: "Washington, DC, USA" or "38.9072, -77.0369"')
    .setPlaceholder('City, Country or lat,lon')
    .setAllowOverride(true);

  // Info section for sheet mode
  config.newInfo()
    .setId('sheet_mode_info')
    .setText('📊 Multiple Locations Mode: Connect a Google Sheet with location data');

  // Sheet-based join fields
  config.newTextInput()
    .setId('sheet_id')
    .setName('Google Sheet ID (Store Join mode)')
    .setHelpText('The ID from your Google Sheets URL (between /d/ and /edit). Sheet should have columns: store_id, address OR lat, lon')
    .setPlaceholder('1abc...xyz')
    .setAllowOverride(false);
  config.newTextInput()
    .setId('sheet_tab')
    .setName('Sheet tab name')
    .setHelpText('Name of the tab containing location data (default: Stores)')
    .setPlaceholder('Stores')
    .setAllowOverride(false);

  config.setDateRangeRequired(true);
  return config.build();
}

// ===== Schema =====
function getSchema(request) {
  return { schema: getFields_(request).build() };
}

function getFields_(request) {
  var fields = cc.getFields();

  // Always include location keys
  fields.newDimension().setId('location_key').setName('Location').setType(types().TEXT);
  fields.newDimension().setId('store_id').setName('Store ID').setType(types().TEXT);

  // Date/Hour grains
  if (request.configParams && request.configParams.data_type === 'hourly') {
    fields.newDimension().setId('datetime').setName('Datetime').setType(types().YEAR_MONTH_DAY_HOUR);
  } else {
    fields.newDimension().setId('date').setName('Date').setType(types().YEAR_MONTH_DAY);
  }

  // Common metrics
  fields.newMetric().setId('temp').setName('Temperature').setAggregation(agg().AVG).setType(types().NUMBER);
  fields.newMetric().setId('feelslike').setName('Feels Like').setAggregation(agg().AVG).setType(types().NUMBER);
  fields.newMetric().setId('humidity').setName('Humidity %').setAggregation(agg().AVG).setType(types().NUMBER);
  fields.newMetric().setId('precip').setName('Precipitation').setAggregation(agg().SUM).setType(types().NUMBER);
  fields.newMetric().setId('snow').setName('Snow').setAggregation(agg().SUM).setType(types().NUMBER);
  fields.newMetric().setId('windspeed').setName('Wind Speed').setAggregation(agg().AVG).setType(types().NUMBER);

  // Descriptive dims
  fields.newDimension().setId('conditions').setName('Conditions').setType(types().TEXT);
  fields.newDimension().setId('icon').setName('Icon').setType(types().TEXT);

  return fields;
}

function types(){ return cc.FieldType; }
function agg(){ return cc.AggregationType; }

// ===== Data =====
function getData(request) {
  var key = request.configParams && request.configParams.api_key;
  if (!key) {
    cc.newUserError()
      .setDebugText('Missing API key in configParams: ' + JSON.stringify(request.configParams))
      .setText('Please enter a valid Visual Crossing API key.')
      .throwException();
  }
  
  var cfg = normalizeConfig_(request.configParams || {});
  var fields = getFields_(request).forIds(request.fields.map(function(f) { return f.name; }));
  var dr = request.dateRange || {};
  var range = computeDateRange_(dr.startDate, dr.endDate, cfg.data_type);
  
  // Check if this is a master dataset request or a widget request
  var isMasterDataset = request.isMasterDataset || false;
  var masterCacheKey = request.masterCacheKey || null;
  
  var rows = [];
  
  if (isMasterDataset) {
    // Create master dataset based on configured parameters
    log_('Creating master dataset', { location: cfg.location, dateRange: range.start + ' to ' + range.end });
    
    if (cfg.source_mode === 'sheet') {
      var stores = loadStoresFromSheet_(cfg.sheet_id, cfg.sheet_tab);
      var batched = groupLocationsForBatch_(stores);
      var results = [];
      batched.forEach(function(batch) {
        var batchResults = fetchVCBatch_(batch, range, cfg, key);
        results.push.apply(results, batchResults);
      });
      rows = results.map(toRow_(fields, cfg.data_type));
    } else {
      var loc = cfg.location || 'Berlin, DE';
      var result = fetchVCSingle_(loc, range, cfg, key);
      rows = result.map(toRow_(fields, cfg.data_type));
    }
    
    // Cache the master dataset using chunked caching
    var masterCacheKey = 'master:' + Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, 
      cfg.location + '|' + range.start + '|' + range.end + '|' + cfg.data_type + '|' + cfg.units);
    
    var cacheResult = cachePutChunked(masterCacheKey, rows, CACHE_MIN*60);
    log_('Master dataset cached', { 
      key: masterCacheKey, 
      records: rows.length, 
      chunks: cacheResult.chunks 
    });
    
  } else if (masterCacheKey) {
    // Use existing master dataset and filter for widget
    log_('Using master dataset for widget', { cacheKey: masterCacheKey });
    
    var masterData = cacheGetChunked(masterCacheKey);
    if (masterData) {
      log_('Master dataset loaded', { records: masterData.length });
      
      // Filter master dataset for this widget's date range
      var filteredData = masterData.filter(function(row) {
        var dateValue = row.values[1]; // Assuming date is second field
        if (dateValue) {
          var dateStr = dateValue.toString();
          var year = dateStr.substring(0, 4);
          var month = dateStr.substring(4, 6);
          var day = dateStr.substring(6, 8);
          var itemDate = year + '-' + month + '-' + day;
          
          return itemDate >= range.start && itemDate <= range.end;
        }
        return false;
      });
      
      rows = filteredData;
      log_('Filtered data for widget', { records: rows.length });
    } else {
      log_('Master dataset not found, falling back to direct API call');
      // Fallback to direct API call
      if (cfg.source_mode === 'sheet') {
        var stores = loadStoresFromSheet_(cfg.sheet_id, cfg.sheet_tab);
        var batched = groupLocationsForBatch_(stores);
        var results = [];
        batched.forEach(function(batch) {
          var batchResults = fetchVCBatch_(batch, range, cfg, key);
          results.push.apply(results, batchResults);
        });
        rows = results.map(toRow_(fields, cfg.data_type));
      } else {
        var loc = cfg.location || 'Berlin, DE';
        var result = fetchVCSingle_(loc, range, cfg, key);
        rows = result.map(toRow_(fields, cfg.data_type));
      }
    }
  } else {
    // Direct API call (fallback)
    log_('Direct API call (no master dataset)');
    
    if (cfg.source_mode === 'sheet') {
      var stores = loadStoresFromSheet_(cfg.sheet_id, cfg.sheet_tab);
      var batched = groupLocationsForBatch_(stores);
      var results = [];
      batched.forEach(function(batch) {
        var batchResults = fetchVCBatch_(batch, range, cfg, key);
        results.push.apply(results, batchResults);
      });
      rows = results.map(toRow_(fields, cfg.data_type));
    } else {
      var loc = cfg.location || 'Berlin, DE';
      var result = fetchVCSingle_(loc, range, cfg, key);
      rows = result.map(toRow_(fields, cfg.data_type));
    }
  }

  log_('Data retrieval complete', { rows: rows.length });

  return {
    schema: fields.build(),
    rows: rows
  };
}

// Helpers
function normalizeConfig_(cfg){
  return {
    data_type: (cfg.data_type || 'daily'),
    source_mode: (cfg.source_mode || 'single'),
    location: cfg.location,
    sheet_id: cfg.sheet_id,
    sheet_tab: cfg.sheet_tab || 'Stores',
    units: cfg.units || 'metric'
  };
}

function computeDateRange_(start, end, dataType){
  // Dates arrive as YYYY-MM-DD format from Looker Studio
  var s = start || '2024-01-01';
  var e = end || '2024-01-31';
  
  // Convert YYYY-MM-DD to YYYY-MM-DD (already in correct format)
  return { 
    start: s, 
    end: e 
  };
}

function loadStoresFromSheet_(sheetId, tab){
  try {
    var rng = Sheets.Spreadsheets.Values.get(sheetId, "'" + tab + "'!A1:Z10000").values || [];
    var header = rng.shift() || [];
    var idx = {
      store_id: header.indexOf('store_id'),
      address: header.indexOf('address'),
      lat: header.indexOf('lat'),
      lon: header.indexOf('lon')
    };
    return rng.map(function(r) { return {
      store_id: r[idx.store_id],
      address: idx.address>=0 ? r[idx.address]: '',
      lat: idx.lat>=0 ? r[idx.lat]: '',
      lon: idx.lon>=0 ? r[idx.lon]: ''
    }; }).filter(function(x) { return x.store_id && (x.address || (x.lat && x.lon)); });
  } catch (e) {
    throwConnectorError('Failed to read Google Sheet. Check ID, tab name, and permissions.');
  }
}

function groupLocationsForBatch_(stores){
  // Visual Crossing Multiple Locations endpoint prefers up to ~50 per call (tune as needed)
  var chunks = [];
  var copy = stores.slice();
  while (copy.length) chunks.push(copy.splice(0, 25));
  return chunks;
}

function buildLocationString_(s){
  if (s.address) return s.address;
  if (s.lat && s.lon) return s.lat + ',' + s.lon;
  return '';
}

function fetchVCBatch_(storesChunk, range, cfg, key){
  log_('Fetching batch data with smart caching', { 
    locations: storesChunk.length, 
    dateRange: range.start + ' to ' + range.end 
  });

  // For batch requests, we'll use the original approach but with better cache keys
  var locs = storesChunk.map(buildLocationString_).join('|');
  var url = VC_BASE + '/timelinemulti?locations=' + encodeURIComponent(locs) + 
            '&datestart=' + range.start + 
            '&dateend=' + range.end + 
            '&unitGroup=' + cfg.units + 
            '&key=' + key;

  // Create a cache key that includes date range to avoid cross-user cache pollution
  var dateRangeHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, range.start + '|' + range.end);
  var locationsHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, locs);
  var cacheKey = 'batch:' + dateRangeHash + ':' + locationsHash + ':' + cfg.units;
  var json = cacheGetChunked(cacheKey);
  if (json) {
    log_('Batch cache hit', { key: cacheKey });
  } else {
    log_('Batch cache miss, fetching from API', { key: cacheKey });
    var res = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    var responseCode = res.getResponseCode();
    var responseText = res.getContentText();
    
    if (responseCode >= 400) {
      cc.newUserError()
        .setDebugText('Visual Crossing API error (batch). URL: ' + url + ', Response Code: ' + responseCode + ', Response: ' + responseText)
        .setText('Visual Crossing API error. Please check your API key and try again.')
        .throwException();
    }
    
    try {
      json = JSON.parse(responseText);
    } catch (parseError) {
      cc.newUserError()
        .setDebugText('Failed to parse Visual Crossing API response. Response: ' + responseText)
        .setText('Invalid response from Visual Crossing API.')
        .throwException();
    }
    
    // Cache the response using chunked caching
    cachePutChunked(cacheKey, json, CACHE_MIN*60);
    log_('Batch data cached', { key: cacheKey });
  }

  // Map results back to store_ids using order (VC returns in same order)
  var out = [];
  if (!json.locations) return out;
  
  json.locations.forEach(function(locObj, i) {
    var store = storesChunk[i];
    if (!locObj || !locObj.days) return;
    
    var locKey = locObj.resolvedAddress || locObj.address || store.address || '';
    
    locObj.days.forEach(function(day) {
      if (cfg.data_type === 'hourly' && day.hours) {
        // Extract hourly data from within each day
        day.hours.forEach(function(h) {
          out.push({ 
            store_id: store.store_id, 
            location_key: locKey, 
            rec: h 
          });
        });
      } else {
        // Daily data
        out.push({ 
          store_id: store.store_id, 
          location_key: locKey, 
          rec: day 
        });
      }
    });
  });
  
  log_('Batch data processing complete', { records: out.length });
  return out;
}

function fetchVCSingle_(location, range, cfg, key){
  log_('Fetching single location data with smart caching', { 
    location: location, 
    dateRange: range.start + ' to ' + range.end 
  });

  // Try to get data from existing cache entries first
  try {
    var cachedData = getCachedDataForRange_(location, range, cfg);
    if (cachedData && cachedData.length > 0) {
      log_('Using cached data for range', { 
        records: cachedData.length, 
        dateRange: range.start + ' to ' + range.end 
      });
      return cachedData;
    }
  } catch (cacheError) {
    log_('Cache lookup failed, falling back to API call', { error: cacheError.toString() });
  }

  // If no cached data covers the range, make API call
  var url = VC_BASE + '/timeline/' + 
            encodeURIComponent(location) + '/' + 
            range.start + '/' + 
            range.end + 
            '?unitGroup=' + cfg.units + 
            '&key=' + key;
  
  log_('Cache miss, fetching from API', { url: url });

  var res = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  var responseCode = res.getResponseCode();
  var responseText = res.getContentText();
  
  if (responseCode >= 400) {
    cc.newUserError()
      .setDebugText('Visual Crossing API error (single). URL: ' + url + ', Response Code: ' + responseCode + ', Response: ' + responseText)
      .setText('Visual Crossing API error. Please check your API key and try again.')
      .throwException();
  }
  
  var json;
  try {
    json = JSON.parse(responseText);
  } catch (parseError) {
    cc.newUserError()
      .setDebugText('Failed to parse Visual Crossing API response. Response: ' + responseText)
      .setText('Invalid response from Visual Crossing API.')
      .throwException();
  }
  
  // Cache the response by month chunks for better reuse
  try {
    cacheDataByMonthChunks_(location, json, cfg);
  } catch (cacheError) {
    log_('Failed to cache data by month chunks', { error: cacheError.toString() });
    // Continue without caching if there's an error
  }
  
  var locKey = json.resolvedAddress || json.address || location;
  var out = [];

  if (json.days && json.days.length > 0) {
    log_('API response received', { 
      days: json.days.length, 
      location: locKey,
      dateRange: json.days[0].datetime + ' to ' + json.days[json.days.length - 1].datetime
    });
  }

  if (!json.days) return out;
  
  json.days.forEach(function(day) {
    if (cfg.data_type === 'hourly' && day.hours) {
      // Extract hourly data from within each day
      day.hours.forEach(function(h) {
        out.push({ 
          store_id: '', 
          location_key: locKey, 
          rec: h 
        });
      });
    } else {
      // Daily data
      out.push({ 
        store_id: '', 
        location_key: locKey, 
        rec: day 
      });
    }
  });
  
  log_('Data processing complete', { records: out.length });
  
  return out;
}

function toRow_(fields, dataType){
  return function(item){
    var r = item.rec || {};
    var values = [];
    
    // Process item data
    
    fields.asArray().forEach(function(f) {
      switch (f.getId()) {
        case 'location_key': values.push(item.location_key || ''); break;
        case 'store_id': values.push(item.store_id || ''); break;
        case 'date': 
          // For daily: datetime is like "2024-01-15"
          var dateValue = (r.datetime && r.datetime.substring(0,10).replace(/-/g,'')) || '';
          values.push(dateValue); 
          break;
        case 'datetime':
          // For hourly: datetime is like "2024-01-15T13:00:00"
          // Convert to YYYYMMDDHH format
          if (r.datetime) {
            var dt = r.datetime.replace(/[-:T]/g, '').slice(0, 10);
            values.push(dt);
          } else {
            values.push('');
          }
          break;
        case 'temp': values.push(num(r.temp)); break;
        case 'feelslike': values.push(num(r.feelslike)); break;
        case 'humidity': values.push(num(r.humidity)); break;
        case 'precip': values.push(num(r.precip)); break;
        case 'snow': values.push(num(r.snow)); break;
        case 'windspeed': values.push(num(r.windspeed)); break;
        case 'conditions': values.push(r.conditions || ''); break;
        case 'icon': values.push(r.icon || ''); break;
        default: values.push('');
      }
    });
    
    return { values: values };
  };
}

function num(x){ 
  if (x === null || x === undefined || x === '') return null;
  return Number(x); 
}

function throwConnectorError(msg){
  cc.newUserError().setDebugText(msg).setText(msg).throwException();
}

// ===== Smart Caching Functions =====

/**
 * Get cached data for a specific date range, checking for overlapping cached periods
 */
function getCachedDataForRange_(location, range, cfg) {
  var locationHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, location + '|' + cfg.units);
  
  log_('Checking for cached data', { 
    location: location, 
    dateRange: range.start + ' to ' + range.end
  });
  
  // Get all cached monthly data
  var allCachedData = [];
  var missingMonths = [];
  
  // Generate list of months that cover the requested range
  var months = getMonthsInRange_(range.start, range.end);
  
  months.forEach(function(month) {
    var monthKey = 'monthly:' + locationHash + ':' + month;
    var monthData = cacheGetChunked(monthKey);
    
    if (monthData && monthData.days && monthData.days.length > 0) {
      log_('Found cached data for month', { month: month, days: monthData.days.length });
      allCachedData.push(monthData);
    } else {
      missingMonths.push(month);
      log_('Missing data for month', { month: month });
    }
  });
  
  if (missingMonths.length > 0) {
    log_('Some months missing from cache', { missing: missingMonths });
    return null; // Return null to trigger API call for missing data
  }
  
  if (allCachedData.length === 0) {
    log_('No cached data found for location', { location: location });
    return null;
  }
  
  // Merge all cached data and filter by date range
  var mergedData = mergeCachedData_(allCachedData, range);
  
  log_('Cached data retrieved and filtered', { 
    totalRecords: mergedData.length,
    dateRange: range.start + ' to ' + range.end,
    months: months.length
  });
  
  return mergedData;
}

/**
 * Cache data by month chunks for better reuse
 */
function cacheDataByMonthChunks_(location, json, cfg) {
  if (!json.days || json.days.length === 0) return;
  
  var locationHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, location + '|' + cfg.units);
  
  // Group days by month
  var monthlyData = {};
  
  json.days.forEach(function(day) {
    var date = new Date(day.datetime);
    var monthKey = date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0');
    
    if (!monthlyData[monthKey]) {
      monthlyData[monthKey] = {
        resolvedAddress: json.resolvedAddress,
        address: json.address,
        days: []
      };
    }
    
    monthlyData[monthKey].days.push(day);
  });
  
  // Cache each month separately
  Object.keys(monthlyData).forEach(function(month) {
    var monthKey = 'monthly:' + locationHash + ':' + month;
    var monthData = monthlyData[month];
    
    cachePutChunked(monthKey, monthData, CACHE_MIN*60);
    log_('Cached monthly data', { 
      month: month, 
      days: monthData.days.length,
      key: monthKey
    });
  });
}

/**
 * Get list of months that cover a date range
 */
function getMonthsInRange_(startDate, endDate) {
  var months = [];
  var start = new Date(startDate);
  var end = new Date(endDate);
  
  var current = new Date(start.getFullYear(), start.getMonth(), 1);
  
  while (current <= end) {
    var monthKey = current.getFullYear() + '-' + String(current.getMonth() + 1).padStart(2, '0');
    months.push(monthKey);
    
    // Move to next month
    current.setMonth(current.getMonth() + 1);
  }
  
  return months;
}

/**
 * Merge cached data from multiple months and filter by date range
 */
function mergeCachedData_(cachedDataArray, range) {
  var allDays = [];
  
  // Collect all days from all cached months
  cachedDataArray.forEach(function(monthData) {
    if (monthData.days) {
      monthData.days.forEach(function(day) {
        allDays.push(day);
      });
    }
  });
  
  // Sort by date
  allDays.sort(function(a, b) {
    return new Date(a.datetime) - new Date(b.datetime);
  });
  
  // Filter by date range
  var filteredDays = allDays.filter(function(day) {
    var dayDate = day.datetime.substring(0, 10); // YYYY-MM-DD
    return dayDate >= range.start && dayDate <= range.end;
  });
  
  // Convert to the expected format
  var result = [];
  var locKey = cachedDataArray.length > 0 ? 
    (cachedDataArray[0].resolvedAddress || cachedDataArray[0].address || '') : '';
  
  filteredDays.forEach(function(day) {
    result.push({
      store_id: '',
      location_key: locKey,
      rec: day
    });
  });
  
  return result;
}

// Required by Looker Studio
function isAdminUser() {
  return false;
}

// ===== CACHE MANAGEMENT FUNCTIONS =====

/**
 * Clear all cache entries
 * @returns {Object} Operation result
 */
function clearCache() {
  try {
    var cache = CacheService.getScriptCache();
    cache.removeAll();
    log_('Cache cleared successfully');
    return { success: true, message: 'Cache cleared' };
  } catch (e) {
    log_('Failed to clear cache', { error: e.toString() });
    return { success: false, error: e.toString() };
  }
}

/**
 * Get cache statistics
 * @returns {Object} Cache statistics
 */
function getCacheStats() {
  try {
    var cache = CacheService.getScriptCache();
    var keys = cache.getKeys();
    var stats = {
      totalKeys: keys.length,
      chunkedKeys: 0,
      normalKeys: 0,
      metadataKeys: 0
    };
    
    keys.forEach(function(key) {
      if (key.includes('_chunk_')) {
        stats.chunkedKeys++;
      } else if (key.includes('_meta')) {
        stats.metadataKeys++;
      } else {
        stats.normalKeys++;
      }
    });
    
    log_('Cache statistics', stats);
    return { success: true, stats: stats };
  } catch (e) {
    log_('Failed to get cache stats', { error: e.toString() });
    return { success: false, error: e.toString() };
  }
}

/**
 * Clear cache entries by pattern
 * @param {string} pattern - Pattern to match cache keys
 * @returns {Object} Operation result
 */
function clearCacheByPattern(pattern) {
  try {
    var cache = CacheService.getScriptCache();
    var keys = cache.getKeys();
    var removedCount = 0;
    
    keys.forEach(function(key) {
      if (key.includes(pattern)) {
        cacheRemoveChunked(key);
        removedCount++;
      }
    });
    
    log_('Cache cleared by pattern', { pattern: pattern, removed: removedCount });
    return { success: true, removed: removedCount };
  } catch (e) {
    log_('Failed to clear cache by pattern', { error: e.toString() });
    return { success: false, error: e.toString() };
  }
}

/**
 * Clear cache for specific date range
 * @param {string} startDate - Start date (YYYY-MM-DD)
 * @param {string} endDate - End date (YYYY-MM-DD)
 * @param {string} location - Location to clear cache for
 * @returns {Object} Operation result
 */
function clearCacheForDateRange(startDate, endDate, location) {
  try {
    var dateRangeHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, startDate + '|' + endDate);
    var locationHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, location);
    
    // Clear single location cache
    var singleKey = 'single:' + dateRangeHash + ':' + locationHash;
    cacheRemoveChunked(singleKey);
    
    // Clear batch cache
    var batchKey = 'batch:' + dateRangeHash;
    cacheRemoveChunked(batchKey);
    
    // Clear master dataset cache
    var masterKey = 'master:' + dateRangeHash;
    cacheRemoveChunked(masterKey);
    
    log_('Cache cleared for date range', { 
      dateRange: startDate + ' to ' + endDate, 
      location: location 
    });
    return { success: true, message: 'Cache cleared for date range' };
  } catch (e) {
    log_('Failed to clear cache for date range', { error: e.toString() });
    return { success: false, error: e.toString() };
  }
}

// ===== COMPREHENSIVE TEST SUITE =====

/**
 * Run all tests in the test suite
 * @param {string} apiKey - Visual Crossing API key
 * @returns {Object} Complete test results
 */
function runTestSuite(apiKey) {
  var testResults = {
    suite: 'Visual Crossing Connector Test Suite',
    timestamp: new Date().toISOString(),
    tests: [],
    summary: {
      total: 0,
      passed: 0,
      failed: 0
    }
  };
  
  // Define test functions to run
  var tests = [
    { name: 'API Connection', func: testAPIConnection, args: [apiKey] },
    { name: 'Data Retrieval', func: testDataRetrieval, args: [apiKey] },
    { name: 'Caching', func: testCaching, args: [apiKey] },
    { name: 'Chunked Caching', func: testChunkedCaching, args: [apiKey] },
    { name: 'Smart Caching', func: testSmartCaching, args: [apiKey] }
  ];
  
  // Run each test
  tests.forEach(function(test) {
    try {
      var result = test.func.apply(null, test.args);
      testResults.tests.push(result);
      testResults.summary.total++;
      
      if (result.success) {
        testResults.summary.passed++;
      } else {
        testResults.summary.failed++;
      }
    } catch (e) {
      testResults.tests.push({
        test: test.name,
        success: false,
        error: e.toString(),
        timestamp: new Date().toISOString()
      });
      testResults.summary.total++;
      testResults.summary.failed++;
    }
  });
  
  log_('Test suite completed', testResults.summary);
  return testResults;
}

/**
 * Run quick connectivity test
 * @param {string} apiKey - Visual Crossing API key
 * @returns {Object} Quick test results
 */
function runQuickTest(apiKey) {
  var results = {
    quickTest: true,
    timestamp: new Date().toISOString(),
    tests: []
  };
  
  // Test API connection
  var apiTest = testAPIConnection(apiKey);
  results.tests.push(apiTest);
  
  // Test basic data retrieval
  var dataTest = testDataRetrieval(apiKey, {
    location: 'London,UK',
    dataType: 'daily',
    units: 'metric',
    startDate: '2024-01-01',
    endDate: '2024-01-07'
  });
  results.tests.push(dataTest);
  
  results.summary = {
    total: results.tests.length,
    passed: results.tests.filter(function(t) { return t.success; }).length,
    failed: results.tests.filter(function(t) { return !t.success; }).length
  };
  
  return results;
}

/**
 * Run performance test
 * @param {string} apiKey - Visual Crossing API key
 * @returns {Object} Performance test results
 */
function runPerformanceTest(apiKey) {
  var startTime = new Date().getTime();
  
  try {
    clearCache();
    
    var testRequest = {
      configParams: {
        api_key: apiKey,
        data_type: 'daily',
        source_mode: 'single',
        location: 'New York',
        units: 'us'
      },
      dateRange: {
        startDate: '2024-01-01',
        endDate: '2024-12-31'
      },
      fields: [
        { name: 'location_key' },
        { name: 'date' },
        { name: 'temp' },
        { name: 'humidity' },
        { name: 'precip' }
      ]
    };
    
    // First call - measure API time
    var apiStartTime = new Date().getTime();
    var result1 = getData(testRequest);
    var apiEndTime = new Date().getTime();
    var apiTime = apiEndTime - apiStartTime;
    
    // Second call - measure cache time
    var cacheStartTime = new Date().getTime();
    var result2 = getData(testRequest);
    var cacheEndTime = new Date().getTime();
    var cacheTime = cacheEndTime - cacheStartTime;
    
    var totalTime = new Date().getTime() - startTime;
    var speedup = apiTime / cacheTime;
    
    return {
      test: 'Performance',
      success: result1.rows.length > 0 && result2.rows.length > 0,
      apiTime: apiTime,
      cacheTime: cacheTime,
      totalTime: totalTime,
      speedup: speedup,
      rows: result1.rows.length,
      cacheEffective: speedup > 1,
      timestamp: new Date().toISOString()
    };
  } catch (e) {
    return {
      test: 'Performance',
      success: false,
      error: e.toString(),
      timestamp: new Date().toISOString()
    };
  }
}

// ===== UTILITY FUNCTIONS =====

/**
 * Get cache keys for a specific location
 * @param {string} location - Location to check
 * @param {string} units - Units (us/metric)
 * @returns {Object} Cache keys for location
 */
function getCacheKeysForLocation(location, units) {
  try {
    var locationHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, location + '|' + units);
    var currentDate = new Date();
    var locationKeys = [];
    
    // Check for the last 12 months
    for (var i = 0; i < 12; i++) {
      var checkDate = new Date(currentDate.getFullYear(), currentDate.getMonth() - i, 1);
      var monthKey = checkDate.getFullYear() + '-' + String(checkDate.getMonth() + 1).padStart(2, '0');
      var fullKey = 'monthly:' + locationHash + ':' + monthKey;
      
      var monthData = cacheGetChunked(fullKey);
      if (monthData && monthData.days && monthData.days.length > 0) {
        locationKeys.push(fullKey);
      }
    }
    
    log_('Cache keys for location', { 
      location: location, 
      keys: locationKeys,
      count: locationKeys.length 
    });
    
    return { success: true, keys: locationKeys };
  } catch (e) {
    log_('Failed to get cache keys for location', { error: e.toString() });
    return { success: false, error: e.toString() };
  }
}

/**
 * Clear cache for a specific location
 * @param {string} location - Location to clear cache for
 * @param {string} units - Units (us/metric)
 * @returns {Object} Operation result
 */
function clearCacheForLocation(location, units) {
  try {
    var locationHash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, location + '|' + units);
    var removedCount = 0;
    
    // Clear monthly cache keys for the last 24 months
    var currentDate = new Date();
    for (var i = 0; i < 24; i++) {
      var checkDate = new Date(currentDate.getFullYear(), currentDate.getMonth() - i, 1);
      var monthKey = checkDate.getFullYear() + '-' + String(checkDate.getMonth() + 1).padStart(2, '0');
      var fullKey = 'monthly:' + locationHash + ':' + monthKey;
      
      try {
        cacheRemoveChunked(fullKey);
        removedCount++;
      } catch (e) {
        // Key might not exist, that's okay
      }
    }
    
    log_('Cache cleared for location', { 
      location: location, 
      removed: removedCount 
    });
    
    return { success: true, removed: removedCount };
  } catch (e) {
    log_('Failed to clear cache for location', { error: e.toString() });
    return { success: false, error: e.toString() };
  }
}

// ===== DEBUG AND DEVELOPMENT FUNCTIONS =====

/**
 * Debug connector request with sample data
 * @param {string} apiKey - Visual Crossing API key
 * @returns {Object} Debug results
 */
function debugConnectorRequest(apiKey) {
  var testRequest = {
    configParams: {
      api_key: apiKey,
      data_type: 'daily',
      source_mode: 'single',
      location: 'London,UK',
      units: 'metric'
    },
    dateRange: {
      startDate: '2024-01-01',
      endDate: '2024-01-07'
    },
    fields: [
      { name: 'location_key' },
      { name: 'date' },
      { name: 'temp' },
      { name: 'conditions' }
    ]
  };
  
  try {
    var result = getData(testRequest);
    log_('Connector debug successful', { rows: result.rows.length });
    return {
      success: true,
      rows: result.rows.length,
      sampleData: result.rows.slice(0, 3), // First 3 rows as sample
      timestamp: new Date().toISOString()
    };
  } catch (e) {
    log_('Connector debug failed', { error: e.toString() });
    return { 
      success: false, 
      error: e.toString(),
      timestamp: new Date().toISOString()
    };
  }
}

// ===== MAIN TEST ENTRY POINTS =====

/**
 * Main test entry point - run all tests
 * Usage: runAllTests('YOUR_API_KEY')
 */
function runAllTests(apiKey) {
  if (!apiKey) {
    log_('No API key provided for testing');
    return { error: 'API key required for testing' };
  }
  
  log_('Starting comprehensive test suite', { apiKey: apiKey.substring(0, 8) + '...' });
  
  var results = {
    timestamp: new Date().toISOString(),
    apiKey: apiKey.substring(0, 8) + '...',
    tests: {}
  };
  
  // Run individual test suites
  results.tests.quick = runQuickTest(apiKey);
  results.tests.performance = runPerformanceTest(apiKey);
  results.tests.full = runTestSuite(apiKey);
  
  // Calculate overall success
  var allTests = [
    results.tests.quick,
    results.tests.performance,
    results.tests.full
  ];
  
  results.summary = {
    totalSuites: allTests.length,
    successfulSuites: allTests.filter(function(suite) { 
      return suite.summary && suite.summary.passed > 0; 
    }).length,
    timestamp: new Date().toISOString()
  };
  
  log_('Test suite completed', results.summary);
  return results;
}

/**
 * Quick test entry point
 * Usage: runQuickTests('YOUR_API_KEY')
 */
function runQuickTests(apiKey) {
  return runQuickTest(apiKey);
}

/**
 * Performance test entry point
 * Usage: runPerformanceTests('YOUR_API_KEY')
 */
function runPerformanceTests(apiKey) {
  return runPerformanceTest(apiKey);
}

// ===== TEST SUITE =====

/**
 * Test Suite for Visual Crossing Looker Studio Connector
 * 
 * This section contains comprehensive test functions to verify:
 * - API connectivity and authentication
 * - Data retrieval and processing
 * - Caching mechanisms
 * - Error handling
 * - Performance optimization
 */

// ===== CORE TEST FUNCTIONS =====

/**
 * Test API connection and authentication
 * @param {string} apiKey - Visual Crossing API key
 * @param {string} testLocation - Location to test (default: 'London,UK')
 * @returns {Object} Test results
 */
function testAPIConnection(apiKey, testLocation) {
    testLocation = testLocation || 'London,UK';
    var testUrl = VC_BASE + '/timeline/' + encodeURIComponent(testLocation) + '/2024-01-01/2024-01-07?unitGroup=metric&key=' + apiKey;
    
    try {
      var res = UrlFetchApp.fetch(testUrl, { muteHttpExceptions: true });
      var responseCode = res.getResponseCode();
      var responseText = res.getContentText();
      
      log_('API connection test', { 
        url: testUrl, 
        status: responseCode,
        success: responseCode === 200 
      });
      
      return {
        test: 'API Connection',
        url: testUrl,
        responseCode: responseCode,
        success: responseCode === 200,
        responseSize: responseText.length,
        timestamp: new Date().toISOString()
      };
    } catch (e) {
      log_('API connection test failed', { error: e.toString() });
      return { 
        test: 'API Connection',
        success: false, 
        error: e.toString(),
        timestamp: new Date().toISOString()
      };
    }
  }
  
  /**
   * Test data retrieval and processing
   * @param {string} apiKey - Visual Crossing API key
   * @param {Object} testConfig - Test configuration
   * @returns {Object} Test results
   */
  function testDataRetrieval(apiKey, testConfig) {
    testConfig = testConfig || {
      location: 'New York',
      dataType: 'daily',
      units: 'us',
      startDate: '2024-08-01',
      endDate: '2024-08-05'
    };
    
    var testRequest = {
      configParams: {
        api_key: apiKey,
        data_type: testConfig.dataType,
        source_mode: 'single',
        location: testConfig.location,
        units: testConfig.units
      },
      dateRange: {
        startDate: testConfig.startDate,
        endDate: testConfig.endDate
      },
      fields: [
        { name: 'location_key' },
        { name: 'date' },
        { name: 'temp' },
        { name: 'conditions' }
      ]
    };
    
    try {
      var result = getData(testRequest);
      log_('Data retrieval test', { 
        rows: result.rows.length, 
        success: result.rows.length > 0 
      });
      
      return {
        test: 'Data Retrieval',
        success: result.rows.length > 0,
        rows: result.rows.length,
        config: testConfig,
        timestamp: new Date().toISOString()
      };
    } catch (e) {
      log_('Data retrieval test failed', { error: e.toString() });
      return { 
        test: 'Data Retrieval',
        success: false, 
        error: e.toString(),
        timestamp: new Date().toISOString()
      };
    }
  }
  
  /**
   * Test caching functionality
   * @param {string} apiKey - Visual Crossing API key
   * @returns {Object} Test results
   */
  function testCaching(apiKey) {
    try {
      // Clear cache first
      clearCache();
      
      var testRequest = {
        configParams: {
          api_key: apiKey,
          data_type: 'daily',
          source_mode: 'single',
          location: 'New York',
          units: 'us'
        },
        dateRange: {
          startDate: '2024-09-25',
          endDate: '2024-09-30'
        },
        fields: [
          { name: 'location_key' },
          { name: 'date' },
          { name: 'temp' }
        ]
      };
      
      // First call - should cache data
      var result1 = getData(testRequest);
      var stats1 = getCacheStats();
      
      // Second call - should use cached data
      var result2 = getData(testRequest);
      var stats2 = getCacheStats();
      
      var isConsistent = result1.rows.length === result2.rows.length;
      
      return {
        test: 'Caching',
        success: isConsistent && result1.rows.length > 0,
        firstCallRows: result1.rows.length,
        secondCallRows: result2.rows.length,
        cacheStats: stats1.stats,
        consistent: isConsistent,
        timestamp: new Date().toISOString()
      };
    } catch (e) {
      log_('Caching test failed', { error: e.toString() });
      return { 
        test: 'Caching',
        success: false, 
        error: e.toString(),
        timestamp: new Date().toISOString()
      };
    }
  }
  
  /**
   * Test chunked caching for large datasets
   * @param {string} apiKey - Visual Crossing API key
   * @returns {Object} Test results
   */
  function testChunkedCaching(apiKey) {
    try {
      clearCache();
      
      var testRequest = {
        configParams: {
          api_key: apiKey,
          data_type: 'hourly',
          source_mode: 'single',
          location: 'New York',
          units: 'us'
        },
        dateRange: {
          startDate: '2024-01-01',
          endDate: '2024-01-31'
        },
        fields: [
          { name: 'location_key' },
          { name: 'datetime' },
          { name: 'temp' },
          { name: 'humidity' },
          { name: 'precip' },
          { name: 'windspeed' },
          { name: 'conditions' }
        ]
      };
      
      // First call - should cache data in chunks
      var result1 = getData(testRequest);
      var stats1 = getCacheStats();
      
      // Second call - should use cached chunks
      var result2 = getData(testRequest);
      
      return {
        test: 'Chunked Caching',
        success: result1.rows.length > 0,
        rows: result1.rows.length,
        cacheStats: stats1.stats,
        chunked: stats1.stats.chunkedKeys > 0,
        timestamp: new Date().toISOString()
      };
    } catch (e) {
      log_('Chunked caching test failed', { error: e.toString() });
      return { 
        test: 'Chunked Caching',
        success: false, 
        error: e.toString(),
        timestamp: new Date().toISOString()
      };
    }
  }
  
  /**
   * Test smart caching with overlapping date ranges
   * @param {string} apiKey - Visual Crossing API key
   * @returns {Object} Test results
   */
  function testSmartCaching(apiKey) {
    try {
      clearCache();
      
      // First request: March to September
      var request1 = {
        configParams: {
          api_key: apiKey,
          data_type: 'daily',
          source_mode: 'single',
          location: 'New York',
          units: 'us'
        },
        dateRange: {
          startDate: '2024-03-01',
          endDate: '2024-09-03'
        },
        fields: [
          { name: 'location_key' },
          { name: 'date' },
          { name: 'temp' }
        ]
      };
      
      var result1 = getData(request1);
      var stats1 = getCacheStats();
      
      // Second request: March to October (should reuse March-September data)
      var request2 = {
        configParams: {
          api_key: apiKey,
          data_type: 'daily',
          source_mode: 'single',
          location: 'New York',
          units: 'us'
        },
        dateRange: {
          startDate: '2024-03-01',
          endDate: '2024-10-02'
        },
        fields: [
          { name: 'location_key' },
          { name: 'date' },
          { name: 'temp' }
        ]
      };
      
      var result2 = getData(request2);
      var stats2 = getCacheStats();
      
      // Third request: July to September (should use cached data)
      var request3 = {
        configParams: {
          api_key: apiKey,
          data_type: 'daily',
          source_mode: 'single',
          location: 'New York',
          units: 'us'
        },
        dateRange: {
          startDate: '2024-07-01',
          endDate: '2024-09-30'
        },
        fields: [
          { name: 'location_key' },
          { name: 'date' },
          { name: 'temp' }
        ]
      };
      
      var result3 = getData(request3);
      
      return {
        test: 'Smart Caching',
        success: result3.rows.length > 0,
        firstRequest: result1.rows.length,
        secondRequest: result2.rows.length,
        thirdRequest: result3.rows.length,
        cacheStatsAfterFirst: stats1.stats,
        cacheStatsAfterSecond: stats2.stats,
        smartCachingWorking: result3.rows.length > 0,
        timestamp: new Date().toISOString()
      };
    } catch (e) {
      log_('Smart caching test failed', { error: e.toString() });
      return { 
        test: 'Smart Caching',
        success: false, 
        error: e.toString(),
        timestamp: new Date().toISOString()
      };
    }
  }
// ===== END OF FILE =====