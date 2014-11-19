
### Serialization Configuration

**Declarative:**

```xml
<serialization>
   <portable-version>???</portable-version>
   <use-native-byte-order>???</use-native-byte-order>
   <byte-order>???</byte-order>
   <enable-compression>???</enable-compression>
   <enable-shared-object>???</enable-shared-object>
   <allow-unsafe>???</allow-unsafe>
   <data-serializable-factories>
      <data-serializable-factory>???</data-serializable-factory>
   </data-serializable-factories>
   <portable-factories>
      <portable-factory>???</portable-factory>
   </portable-factories>
   <serializers>
      <global-serializer>???</global-serializer>
      <serializer>???</serializer>
   </serializers>
   <check-class-def-errors>???</check-class-def-errors>
</serialization>
```

**Programmatic:**

```java
Config config = new Config();
SerializationConfig serializationConfig = config.getSerializationConfig();
serializationConfig.setPortableVersion( "???" ).setUseNativeByteOrder( "???" )
          .setAllowUnsafe( "???" );
```

It has below attributes.

- portable-version: Defines the versioning of the portable serialization. Portable version will be used to differentiate two same classes that have changes on it like adding/removing field or changing a type of a field.
- use-native-byte-order: Set to true to use native byte order of the underlying platform. 
- byte-order: Defines the byte order that the serialization will use. 
- enable-compression: Enables compression if default Java serialization is used. 
- enable-shared-object: Enables shared object if default Java serialization is used. 
- allow-unsafe: Set to `true` to allow the usage of Unsafe. 
- data-serializable-factory: DataSerializableFactory class to be registered.
- portable-factory: PortableFactory class to be registered.
- global-serializer: Global serializer class to be registered if no other serializer is applicable.
- serializer: Defines the class name of the serializer implementation.
- check-class-def-errors: When enabled, serialization system will check class definitions error at start and throw an Serialization Exception with error definition.



