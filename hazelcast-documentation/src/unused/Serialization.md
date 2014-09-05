

# Serialization

All your distributed objects such as your key and value objects, objects you offer into distributed queue and your distributed callable/runnable objects have to be `Serializable`.

Hazelcast serializes all your objects into an instance of `com.hazelcast.nio.serialization.Data`. `Data` is the binary representation of an object. 

When Hazelcast serializes an object into `Data`, it first checks whether the object is an instance of `com.hazelcast.nio.serialization.DataSerializable`, if not it checks if it is an instance of `com.hazelcast.nio.serialization.Portable` and serializes it accordingly. 

Hazelcast optimizes the serialization for the below types, and the user cannot override this behavior:

-	`Byte`
-	`Boolean`
-	`Character`
-	`Short`
-	`Integer`
-	`Long`
-	`Float`
-	`Double`
-	`byte[]`
-	`char[]`
-	`short[]`
-	`int[]`
-	`long[]`
-	`float[]`
-	`double[]`
-	`String`

Hazelcast also optimizes the following types. However, you can override them by creating a custom serializer and registering it. See [Custom Serialization](#custom-serialization) for more information.

-   `Date`
-   `BigInteger`
-   `BigDecimal`
-   `Class`
-   `Externalizable`
-   `Serializable`

Note that, if the object is not an instance of any explicit type, Hazelcast uses Java Serialization for Serializable and Externalizable objects. The default behavior can be changed using a [Custom Serialization](#custom-serialization).
