



## Serialization Interfaces
 
When it comes to complex objects, below interfaces are used for serialization and deserialization.

- `java.io.Serializable`
- `java.io.Externalizable`

- `com.hazelcast.nio.serialization.DataSerializable`

- `com.hazelcast.nio.serialization.IdentifiedDataSerializable`

- `com.hazelcast.nio.serialization.Portable`, and
- Custom Serialization (using `StreamSerializer`, `ByteArraySerializer`)


When Hazelcast serializes an object into `Data`:

**(1)** It first checks whether the object is an instance of `com.hazelcast.nio.serialization.DataSerializable`, 

**(2)** If above fails, then it checks if it is an instance of `com.hazelcast.nio.serialization.Portable`,

**(3)** If above fails, then it checks whether the object is of a well-known type like String, Long, Integer, etc. and user specified types like ByteArraySerializer or StreamSerializer,

**(4)** If above checks fail, Hazelcast will use Java serialization.

If all of the above checks do not work, then serialization will fail. When a class implements multiple interfaces, above steps are important to determine the serialization mechanism that Hazelcast will use. And when a class definition is required for any of these serializations, all the classes needed by the application should be on the classpath, Hazelcast does not download them automatically.

