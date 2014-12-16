
## Serialization Configuration

The following are the example configurations.

**Declarative:**

```xml
<serialization>
   <portable-version>2</portable-version>
   <use-native-byte-order>true</use-native-byte-order>
   <byte-order>BIG_ENDIAN</byte-order>
   <enable-compression>true</enable-compression>
   <enable-shared-object>false</enable-shared-object>
   <allow-unsafe>true</allow-unsafe>
   <data-serializable-factories>
      <data-serializable-factory factory-id="1001">
          abc.xyz.Class
      </data-serializable-factory>
   </data-serializable-factories>
   <portable-factories>
      <portable-factory factory-id="9001">
         xyz.abc.Class
      </portable-factory>
   </portable-factories>
   <serializers>
      <global-serializer>abc.Class</global-serializer>
      <serializer type-class="Employee" class-name="com.EmployeeSerializer">
      </serializer>
   </serializers>
   <check-class-def-errors>true</check-class-def-errors>
</serialization>
```

**Programmatic:**

```java
Config config = new Config();
SerializationConfig srzConfig = config.getSerializationConfig();
srzConfig.setPortableVersion( "2" ).setUseNativeByteOrder( true );
srzConfig.setAllowUnsafe( true ).setEnableCompression( true );
srzConfig.setCheckClassDefErrors( true );

GlobalSerializerConfig globSrzConfig = srzConfig.getGlobalSerializerConfig();
globSrzConfig.setClassName( "abc.Class" );

SerializerConfig serializerConfig = srzConfig.getSerializerConfig();
serializerConfig.setTypeClass( "Employee" )
                .setClassName( "com.EmployeeSerializer" );
```

It has below elements.

- `portable-version`: Defines the versioning of the portable serialization. Portable version will be used to differentiate two same classes that have changes on it like adding/removing field or changing a type of a field.
- `use-native-byte-order`: Set to true to use native byte order of the underlying platform. 
- `byte-order`: Defines the byte order that the serialization will use. Available values are `BIG_ENDIAN` and `LITTLE_ENDIAN`. The default value is `BIG_ENDIAN`.
- `enable-compression`: Enables compression if default Java serialization is used. 
- `enable-shared-object`: Enables shared object if default Java serialization is used. 
- `allow-unsafe`: Set to `true` to allow the usage of `unsafe`. 
- `data-serializable-factory`: DataSerializableFactory class to be registered.
- `portable-factory`: PortableFactory class to be registered.
- `global-serializer`: Global serializer class to be registered if no other serializer is applicable.
- `serializer`: Defines the class name of the serializer implementation.
- `check-class-def-errors`: When enabled, serialization system will check class definitions error at start and throw an Serialization Exception with error definition.



