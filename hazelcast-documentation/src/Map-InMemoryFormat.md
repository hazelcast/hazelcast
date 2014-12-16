



### In Memory Format

IMap has an `in-memory-format` configuration option. By default, Hazelcast stores data into memory in binary (serialized) format. But sometimes, it can be efficient to store the entries in their object form, especially in cases of local processing like entry processor and queries. By setting `in-memory-format` in map's configuration, you can decide how the data will be stored in memory. You have the following format options.

-   `BINARY` (default): This is the default option. The data will be stored in serialized binary format. You can use this option if you mostly perform regular map operations, such as `put` and `get`.

-   `OBJECT`: The data will be stored in deserialized form. This configuration is good for maps where entry processing and queries form the majority of all operations and the objects are complex ones, making the serialization cost respectively high. By storing objects, entry processing will not contain the deserialization cost.


Regular operations like `get` rely on the object instance. When the `OBJECT` format is used and a `get` is performed, the map does not return the stored instance, but creates a clone. Therefore, this whole `get` operation includes a serialization first on the node owning the instance, and then a deserialization on the node calling the instance. When the `BINARY` format is used, only a deserialization is required; this is faster.

Similarly, a `put` operation is faster when the `BINARY` format is used. If the format was `OBJECT`, map would create a clone of the instance, and there would first a serialization and then deserialization. When BINARY is used, only a deserialization is needed.


![image](images/NoteSmall.jpg) ***NOTE:*** *If a value is stored in `OBJECT` format, a change on a returned value does not affect the stored instance. In this case, the returned instance is not the actual one but a clone. Therefore, changes made on an object after it is returned will not reflect on the actual stored data. Similarly, when a value is written to a map and the value is stored in `OBJECT` format, it will be a copy of the `put` value. Therefore, changes made on the object after it is stored will not reflect on the stored data.*