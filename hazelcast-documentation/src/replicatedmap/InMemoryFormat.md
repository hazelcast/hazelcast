
### In Memory Format on ReplicatedMap

Currently two in-memory-format values are usable with the ReplicatedMap.

-   **OBJECT (default):**The data will be stored in de-serialized form. This configuration is the default choice since
data replication is mostly used for high speed access. Please be aware, that changing values without a `Map::put` are
not reflected on other nodes but are visible on the changing nodes for later value accesses.

-   **BINARY:**The data will be stored in serialized binary format and have to be deserialized on every request. This
option offers higher encapsulation since changes to values are always discarded as long as the new changes object is
not explicitly `Map::put` into the map again.
