


# Serialization

All your distributed objects such as your key and value objects, objects you offer into distributed queue and your distributed callable/runnable objects need to be serialized.

Hazelcast serializes all your objects into an instance of `com.hazelcast.nio.serialization.Data`. `Data` is the binary representation of an object. 

When Hazelcast serializes an object into `Data`:

**(1)** It first checks whether the object is an instance of `com.hazelcast.nio.serialization.DataSerializable`, 

**(2)** If above fails, then it checks if it is an instance of `com.hazelcast.nio.serialization.Portable`,

**(3)** If above fails, then it checks whether the object is of a well-known type like String, Long, Integer, etc. and user specified types like ByteArraySerializer or StreamSerializer,

**(4)** If above checks fail, Hazelcast will use Java serialization.

If all of the above checks do not work, then serialization will fail. When a class implements multiple interfaces, above steps are important to determine the serialization mechanism that Hazelcast will use. And when a class definition is required for any of these serializations, all the classes needed by the application should be on the classpath, Hazelcast does not download them automatically.

Hazelcast optimizes the serialization for the below types, and the user cannot override this behavior:

- `Byte`
- `Boolean`
- `Character`
- `Short`
- `Integer`
- `Long`
- `Float`
- `Double`
- `byte[]`
- `char[]`
- `short[]`
- `int[]`
- `long[]`
- `float[]`
- `double[]`
- `String`

Hazelcast also optimizes the following types. However, you can override them by creating a custom serializer and registering it. See [Custom Serialization](#custom-serialization) for more information.

- `Date`
- `BigInteger`
- `BigDecimal`
- `Class`
- `Externalizable`
- `Serializable`

Note that, if the object is not an instance of any explicit type, Hazelcast uses Java Serialization for Serializable and Externalizable objects. The default behavior can be changed using a [Custom Serialization](#custom-serialization).

Let's dig into the details of the above serialization mechanisms in the following sections.

