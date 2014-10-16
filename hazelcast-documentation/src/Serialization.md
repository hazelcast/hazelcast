


# Serialization

## Serialization Overview

Java objects that you put into Hazelcast need to be serialized since Hazelcast is a distributed system. The data and its replicas are stored in different partitions on multiple nodes. The data you need may not be present on the local machine and Hazelcast retrieves that data from another machine. And this requires serialization.

Hazelcast serializes all your objects into an instance of `com.hazelcast.nio.serialization.Data`. `Data` is the binary representation of an object. 

Serialization is used when;

- key/value objects are added to a map,
- items are put in a queue/set/list,
- a runnable is sent using an executor service,
- an entry processing is performed within a map,
- an object is locked,and
- a message is sent to a topic


Hazelcast optimizes the serialization for the below types, and the user cannot override this behavior:

![image](images/OptimizedTypes.jpg)


Hazelcast also optimizes the following types. However, you can override them by creating a custom serializer and registering it. See [Custom Serialization](#custom-serialization) for more information.

![image](images/OptimizedTypesII.jpg)


As we have said, Hazelcast optimizes all of the above object types and you do not need to worry about their (de)serializations.

