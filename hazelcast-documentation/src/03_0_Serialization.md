

# Serialization

All your distributed objects such as your key and value objects, objects you offer into distributed queue and your distributed callable/runnable objects have to be`Serializable`.

Hazelcast serializes all your objects into an instance of`com.hazelcast.nio.serialization.Data`. `Data` is the binary representation of an object. When Hazelcast serializes an object into`Data`, it first checks whether the object is an instance of `com.hazelcast.nio.serialization.DataSerializable`, if not it checks if it is an instance of `com.hazelcast.nio.serialization.Portable` and serializes it accordingly. For the following types Hazelcast optimizes the serialization a user can not override this behaviour. `Byte`, `Boolean`, `Character`, `Short`, `Integer`, `Long`, `Float`, `Double`, `byte[]`, `char[]`, `short[]`, `int[]`, `long[]`, `float[]`, `double[]`, `String`, Hazelcast also optimizes the following types, however you can override them by creating a custom serializer and registering it. See [Custom Serialization](#custom-serialization) for more information.

-   Date
-   BigInteger
-   BigDecimal
-   Class
-   Externalizable
-   Serializable

Not that if the object is not instance of any explicit type, Hazelcast uses Java Serialization for Serializable and Externalizable objects. The default behaviour can be changed using a [Custom Serialization](#custom-serialization).
