

### In Memory Format

With version 3.0, in-memory-format configuration option has been added to distributed map. By default Hazelcast stores data into memory in binary (serialized) format. But sometimes it can be efficient to store the entries in their objects form especially in cases of local processing like entry processor and queries. Setting in-memory-format in map's configuration, you can decide how the data will be store in memory. There are three options.

-   **BINARY (default):**This is the default option. The data will be stored in serialized binary format.

-   **OBJECT:**The data will be stored in de-serialized form. This configuration is good for maps where entry processing and queries form the majority of all operations and the objects are complex ones so serialization cost is respectively high. By storing objects, entry processing will not contain the de-serialization cost.

-   **CACHED:**This option is useful if your map's main use case is the queries. Internally data is in both binary and de-serialized form so queries do not handle serialization.


