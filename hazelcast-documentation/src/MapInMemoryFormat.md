

### In Memory Format

Distributed map has in-memory-format configuration option. By default, Hazelcast stores data into memory in binary (serialized) format. But sometimes, it can be efficient to store the entries in their objects form, especially in cases of local processing like entry processor and queries. Setting in-memory-format in map's configuration, you can decide how the data will be stored in memory. There are below options.

-   **BINARY (default)**: This is the default option. The data will be stored in serialized binary format.

-   **OBJECT**: The data will be stored in de-serialized form. This configuration is good for maps where entry processing and queries form the majority of all operations and the objects are complex ones, so serialization cost is respectively high. By storing objects, entry processing will not contain the de-serialization cost.


