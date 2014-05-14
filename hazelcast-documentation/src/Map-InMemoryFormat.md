



### In Memory Format

IMap has `in-memory-format` configuration option. By default, Hazelcast stores data into memory in binary (serialized) format. But sometimes, it can be efficient to store the entries in their object form, especially in cases of local processing like entry processor and queries. Setting `in-memory-format` in map's configuration, you can decide how the data will be stored in memory. There are below options.

-   **BINARY (default)**: This is the default option. The data will be stored in serialized binary format. You can use this option if you mostly perform regular map operations like put and get.

-   **OBJECT**: The data will be stored in deserialized form. This configuration is good for maps where entry processing and queries form the majority of all operations and the objects are complex ones, so serialization cost is respectively high. By storing objects, entry processing will not contain the deserialization cost.


Regular operations like `get` rely on the object instance. When OBJECT format is used and, for example, when a `get` is performed, the map does not return the stored instance, but creates a clone. So, this whole `get` operation includes a serialization first (on the node owning the instance) and then deserialization (on the node calling the instance). But, when BINARY format is used, only a deserialization is required and this is faster.

Similarly, `put` operation is faster when BINARY format is used. If it was OBJECT, map would create a clone of the instance. So, there would first a serialization and then deserialization. Again, when BINARY is used, only a deserialization is needed.


***<font color="red">Note:</font>*** *If a value is stored in OBJECT format, a change on a returned value does not effect the stored instance. In this case, the returned instance is not the actual one but a clone. Therefore, changes made on an object after it is returned will not reflect on the actual stored data. Similarly, when a value is written to a map and the value is stored in OBJECT format, it will be a copy of the put value. So changes made on the object after it is stored, will not reflect on the actual stored data.*