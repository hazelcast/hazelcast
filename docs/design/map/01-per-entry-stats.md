# IMap Per Entry Stats Config Option 

|ℹ️ Since: 4.2|
|-------------|

## Background

### Description

After implementing per config record creation, we managed to
break tight coupling between features and record metadata.
Based on this, now we have a chance to enable/disable
record metadata to reduce memory usage of a record.

## Design

We categorize stats in two types: map-level and entry-level.
Map level is enabled by default but entry-level is not.

Map-level stats are represented with class `LocalRecordStoreStatsImpl`
It has 3 fields: `hits-lastAccess-lastUpdate`

Entry-level stats are: `hits-lastAccessTime-lastUpdateTime-creationTime-lastStoredTime`
 
### Replication of Map Level Stats
Map-level stats are created per map's partition. They are
replicated to new nodes to be safe from partition ownership changes.

Entry-level stats are already replicated
when enabled. No change is needed for them.

### Cases That Entry Level Stats Must Be Enabled
- Custom eviction policies 
- Entry-views
- For Some Split brain merge policies


 
 
 



