
## Comparison Table

Below table provides a comparison between the interfaces listed in the previous section to help you in deciding which interface to use in your applications.

*Serialization Interface*|*Advantages*|*Drawbacks*
:-|:-|:-
**Serializable**|- A standard and basic Java interface <br> - No implementation is required |- More time and CPU usage <br> - More space occupancy
**Externalizable**|- A standard Java interface <br> - More efficient than Serializable when it comes to CPU and memory usage| - Serialization interface must be implemented
**DataSerialiazable**| - More efficient than Serializable when it comes to CPU and memory usage| - Specific to Hazelcast
**IdentifiedDataSerializable**| - More efficient than Serializable when it comes to CPU and memory usage <br> - Reflection is not used during deserialization| - Specific to Hazelcast <br> - Serialization interface must be implemented <br> - A Factory and configuration must be implemented
**Portable**| - More efficient than Serializable when it comes to CPU and memory usage <br> - Reflection is not used during deserialization <br> - Versioning is supported <br> Partial deserialization is supported during Queries| - Specific to Hazelcast <br> - Serialization interface must be implemented <br> - A Factory and configuration must be implemented <br> - Class definition is also sent with data but stored only once per class
**Custom Serialization**| - Does not require class to implement an interface <br> - Convenient and flexible <br> - Can be based on StreamSerializer ByteArraySerializer|- Serialization interface must be implemented <br> - Plug in and configuration is required



Let's dig into the details of the above serialization mechanisms in the following sections.

