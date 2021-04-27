# New Serialization Format API

|ℹ️ Since 4.2  Beta |
|--------------------|

# Background

This work is intended to introduce a new format that will supersede Portable. The main goals are as follows: 

- Ease of use: The format should require as little configuration as possible.         
- Non-Java friendly: The format has to be easy to use also for non-Java developers.   
- Space Efficiency: The format should be more efficient than Portable. In Portable, we store also field names. We can eliminate this to provide a more compact format.      
- Schema Evolution: The format should support schema evolution.    
- Querying friendly/Partial deserialization: The format should allow partial deserialization to provide optimal performance on queries.

This document will contains background and also API interaction. We also have following related documents.
[Wire Level Format Specification](02-Format-Specification.md)

# API Interaction 

API usage will differ from use case to use case. In this document, we will try to describe each use case and how will the usage looks like. Sample domain class to be used in examples:
```
package com.sample.domainclasses;

public class Employee {
    private String name;
    private int age;

    public Employee(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Employee() {

    }
    
    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }
}
```

1. Simplest use-case is java only and the domain class is in our class-path.
For Employee object to be serialized in the new format, one of the following two should be satisfied: 
    1. It should not implement Portable, IdentifiedDataSerializable, DataSerializable, Serializable, and the user should not configure a global serializer.  This is to ensure backward compatibility. In any existing code migrating from an old version, the serialization method should not change for existing objects.
        ```
         HazelcastInstance client = HazelcastClient.newHazelcastClient();
         IMap<Object, Object> map = instance.getMap("map");
         map.put(1, new Employee("John", 20));
         Employee employee = (Employee) map.get(1);
        ```
    2. User should register Employee class explicitly. This API will be useful when the user does not have write-access to the `Employee` class, but using it from another library.      
        ```
        ClientConfig config = new ClientConfig();
        CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.register(Employee.class);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee("John", 20));
        Employee employee = (Employee) map.get(1);
        ```
    For this use case, the `Employee` class must have an empty constructor.  
    All non-transient fields will be serialized. Any field with a type that is not natively supported in this format
    ( see [Compact Reader API](#CompactReader) for supported types) will be serialized recursively using reflection
    in the new format. So any custom field of `Employee` class also required to have an empty constructor.

2. A second use case is when the domain class is not in our class path
    1. In case of read, since the user object can not be created, `GenericRecord` will be returned instead: 
        ``` 
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> map = instance.getMap("map");
        GenericRecord employee = (GenericRecord) map.get(1);
        int age = employee.readInt("age");
        String name = employee.readUTF("name")
        ```
       
    2. In the case of a write, we will create a `GenericRecord` that represents our class. 
       The only addition to the existing GenericRecord API is `GenericRecordBuilder.compact(String className)` method.
        ```
       HazelcastInstance client = HazelcastClient.newHazelcastClient();
       IMap<Object, Object> map = instance.getMap("map");

       GenericRecord genericRecord = GenericRecordBuilder.compact("employee")
               .writeUTF("name", "John")
               .writeInt("age", 20).build();
       map.put(1, genericRecord);
       GenericRecord person = (GenericRecord) map.get(1);
        ```
       This usage also a good fit for EntryProcessor use case where the sender has the class but the class is not available on the remote cluster.
        ```
        map.executeOnKey(1, new EntryProcessor<Object, Object, Object>() {
           @Override
           public Object process(Map.Entry<Object, Object> entry) {
               GenericRecord record = (GenericRecord) entry.getKey();
               GenericRecord genericRecord = record.cloneWithBuilder()
                       .writeInt("age", 21).build();
               entry.setValue(genericRecord);
               return true;
           }
        });
       ```
3. The use case when it needs to be interoperable between java and non-java will require some additional configuration.
Although it is not a must, it is advised to give an alias class name. The default class name includes the package name of the java class, which is not friendly to be used by non-java clients.
It is advised to give explicit serializer. This way it is simpler to match types of the fields.
When a user does not give an explicit serializer, we will try to create a reflective serializer but not straightforward in most of the languages.
    - C++ does not have this.
    - Node js does not have different types for integer, long, etc. Not clear in which type we should write a number.
    ```
    ClientConfig config = new ClientConfig();
    CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
    compactSerializationConfig.register(Employee.class, "employee", new CompactSerializer<Employee>() {
                   @Override
                   public Employee read(CompactReader in) throws IOException {
                       String name = in.readUTF("name");
                       int age = in.readInt("age");
                       return new Employee(name, age);
                   }
    
                   @Override
                   public void write(CompactWriter out, Employee object) throws IOException {
                       out.writeUTF("name", object.getName());
                       out.writeInt("age", object.getAge());
                   }
               });
                      
    HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
    IMap<Object, Object> map = instance.getMap("map");
    map.put(1, new Employee());
    Employee employee = (Employee) map.get(1);      
    ```
4. One other use case is when a class is evolved. Added/removed some field or a type of the field is changed.
   Let's say that we have added the `surname` field to our `Employee` class later. 
    1. The basic example where a user does not give any serializer will work. We will be able to write any version of the `Employee`
        to the cluster.
        ```
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee("John", 20, "Smith"));
       ```
       For the read, we have 3 possible states. 
        - A field `Employee` has could be removed in the new data.
        - A new field could be added, which the local `Employee` class does not have.
        - A field type could change which does not fit to `Employee`. This is the same as removing a field and adding a new unrelated field.   
       ```
        Employee employee = (Employee) map.get(1);
        ```
       If the incoming data(evolved class on the cluster) have an extra field, the user's object will be created with the fields it has.
       New fields will just be ignored.  
       
       If the incoming data(evolved class on the cluster) have removed a field/changed the type of the field then the related field of the `Employee` object will be left unset. So if the `Employee` class is constructed with some default values in the 
       default constructor, they will be untouched.  
    2.  Another use case is when a user configures an explicit serializer and the data evolves.    
       The user should plan ahead in this case. Any field that could be removed in the future, should be read by providing a default value. Note that `read*(fieldName)` methods throw HazelcastSerializationException when the field is not found.   
        ```
        ClientConfig config = new ClientConfig();
        CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
        compactSerializationConfig.register(Employee.class, "employee", new CompactSerializer<Employee>() {
           @Override
           public Employee read(Schema CompactReader in) throws IOException {
               String name = in.readUTF("name");
               int age = in.readInt("age");
               String surname = in.readUTF("surname", "NOT AVAILABLE");
               return new Employee(name, age, surname);                              
           }
        
           @Override
           public void write(CompactWriter out, Employee object) throws IOException {
               out.writeUTF("name", object.getName());
               out.writeInt("age", object.getAge());
               out.writeUTF("surname", object.getSurname());
           }
        });
        
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> map = instance.getMap("map");
        map.put(1, new Employee());
        Employee employee = (Employee) map.get(1);
        ```
        The example above could be interpreted as the new version of the application which will work with an existing cluster. In the version, we just added a new field, and existing data on the cluster is missing the `surname` field.
    3.  It could be the case that the class is not in our class path. The `GenericRecord` could also be used with the schema evolution as follows:
        ```
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Object, Object> map = instance.getMap("map");

        GenericRecord genericRecord = GenericRecordBuilder.compact("employee")
                .writeUTF("name", "John")
                .writeInt("age", 20)
                .writeUTF("surname", "Smith").build();
        map.put(1, genericRecord);
        GenericRecord employee = (GenericRecord) map.get(1);

        String name = employee.readUTF("name");
        int age = employee.readInt("age");
        String surname ;
        if(employee.hasField("surname") && employee.getFieldType("surname").equals(FieldType.UTF)) {
            surname = employee.readUTF("surname");
        } else {
            surname = "NOT AVAILABLE";
        }
        ```      
# API Design

Here, we will list all the new classes and methods that are necessary for the new format API. 

### CompactSerializer

```
public interface CompactSerializer<T> {
    /**
     * @param in     reader to read fields of an object
     * @return the object created as a result of read method
     * @throws IOException
     */
    T read(CompactReader in) throws IOException;

    /**
     * @param out    CompactWriter to serialize the fields onto
     * @param object to be serialized
     * @throws IOException
     */
    void write(CompactWriter out, T object) throws IOException;
}
```

### CompactReader

Reader and Writer API's is very likely to be changed during the design. For example, new methods for more types could be added.

```
/**
 * All read(String fieldName) methods throw HazelcastSerializationException when the related field is not found or there is a type mismatch.
 * To avoid exception when the field is not found, the user can make use of `read(String fieldName, T defaultValue)`.
 * Especially useful, when class is evolved  (a new field is added to/removed from the class).
 */
public interface CompactReader {

    byte readByte(String fieldName);
    byte readByte(String fieldName, byte defaultValue);

    short readShort(String fieldName);
    short readShort(String fieldName, short defaultValue);

    int readInt(String fieldName);
    int readInt(String fieldName, int defaultValue);

    long readLong(String fieldName);
    long readLong(String fieldName, long defaultValue);

    float readFloat(String fieldName);
    float readFloat(String fieldName, float defaultValue);

    double readDouble(String fieldName);
    double readDouble(String fieldName, double defaultValue);

    boolean readBoolean(String fieldName);
    boolean readBoolean(String fieldName, boolean defaultValue);

    char readChar(String fieldName);
    char readChar(String fieldName, char defaultValue);

    String readUTF(String fieldName);
    String readUTF(String fieldName, String defaultValue);

    BigInteger readBigInteger(String fieldName);
    BigInteger readBigInteger(String fieldName, BigInteger defaultValue);

    BigDecimal readBigDecimal(String fieldName);
    BigDecimal readBigDecimal(String fieldName, BigDecimal defaultValue);

    LocalTime readLocalTime(String fieldName);
    LocalTime readLocalTime(String fieldName, LocalTime defaultValue);

    LocalDate readLocalDate(String fieldName);
    LocalDate readLocalDate(String fieldName, LocalDate defaultValue);

    LocalDateTime readLocalDateTime(String fieldName);
    LocalDateTime readLocalDateTime(String fieldName, LocalDateTime defaultValue);

    OffsetDateTime readOffsetDateTime(String fieldName);
    OffsetDateTime readOffsetDateTime(String fieldName, OffsetDateTime defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    <T> T readObject(String fieldName);
    <T> T readObject(String fieldName, T defaultValue);

    byte[] readByteArray(String fieldName);
    byte[] readByteArray(String fieldName, byte[] defaultValue);

    boolean[] readBooleanArray(String fieldName);
    boolean[] readBooleanArray(String fieldName, boolean[] defaultValue);

    char[] readCharArray(String fieldName);
    char[] readCharArray(String fieldName, char[] defaultValue);

    int[] readIntArray(String fieldName);
    int[] readIntArray(String fieldName, int[] defaultValue);

    long[] readLongArray(String fieldName);
    long[] readLongArray(String fieldName, long[] defaultValue);

    double[] readDoubleArray(String fieldName);
    double[] readDoubleArray(String fieldName, double[] defaultValue);

    float[] readFloatArray(String fieldName);
    float[] readFloatArray(String fieldName, float[] defaultValue);

    short[] readShortArray(String fieldName);
    short[] readShortArray(String fieldName, short[] defaultValue);

    String[] readUTFArray(String fieldName);
    String[] readUTFArray(String fieldName, String[] defaultValue);

    BigInteger[] readBigIntegerArray(String fieldName);
    BigInteger[] readBigIntegerArray(String fieldName, BigInteger[] defaultValue);

    BigDecimal[] readBigDecimalArray(String fieldName);
    BigDecimal[] readBigDecimalArray(String fieldName, BigDecimal[] defaultValue);

    LocalTime[] readLocalTimeArray(String fieldName);
    LocalTime[] readLocalTimeArray(String fieldName, LocalTime[] defaultValue);

    LocalDate[] readLocalDateArray(String fieldName);
    LocalDate[] readLocalDateArray(String fieldName, LocalDate[] defaultValue);

    LocalDateTime[] readLocalDateTimeArray(String fieldName);
    LocalDateTime[] readLocalDateTimeArray(String fieldName, LocalDateTime[] defaultValue);

    OffsetDateTime[] readOffsetDateTimeArray(String fieldName);
    OffsetDateTime[] readOffsetDateTimeArray(String fieldName, OffsetDateTime[] defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    Object[] readObjectArray(String fieldName);
    Object[] readObjectArray(String fieldName, Object[] defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    <T> List<T> readObjectList(String fieldName);
    <T> List<T> readObjectList(String fieldName, List<T> defaultValue);

}
```

### CompactWriter

```
public interface CompactWriter {

    void writeInt(String fieldName, int value);

    void writeLong(String fieldName, long value);

    void writeUTF(String fieldName, String value);

    void writeBoolean(String fieldName, boolean value);

    void writeByte(String fieldName, byte value);

    void writeChar(String fieldName, char value);

    void writeDouble(String fieldName, double value);

    void writeFloat(String fieldName, float value);

    void writeShort(String fieldName, short value);
    
    <T> void writeObject(String fieldName, T value);

    void writeBigInteger(String fieldName, BigInteger value);

    void writeBigDecimal(String fieldName, BigDecimal value);

    void writeLocalTime(String fieldName, LocalTime value);

    void writeLocalDate(String fieldName, LocalDate value);

    void writeLocalDateTime(String fieldName, LocalDateTime value);

    void writeOffsetDateTime(String fieldName, OffsetDateTime value);

    void writeByteArray(String fieldName, byte[] value);

    void writeBooleanArray(String fieldName, boolean[] booleans);

    void writeCharArray(String fieldName, char[] value);

    void writeIntArray(String fieldName, int[] value);

    void writeLongArray(String fieldName, long[] value);

    void writeDoubleArray(String fieldName, double[] value);

    void writeFloatArray(String fieldName, float[] value);

    void writeShortArray(String fieldName, short[] value);

    void writeUTFArray(String fieldName, String[] value);

    void writeBigIntegerArray(String fieldName, BigInteger[] values);

    void writeBigDecimalArray(String fieldName, BigDecimal[] values);

    void writeLocalTimeArray(String fieldName, LocalTime[] values);

    void writeLocalDateArray(String fieldName, LocalDate[] values);

    void writeLocalDateTimeArray(String fieldName, LocalDateTime[] values);

    void writeOffsetDateTimeArray(String fieldName, OffsetDateTime[] values);

    <T> void writeObjectArray(String fieldName, T[] value);

    <T> void writeObjectList(String fieldName, List<T> list);

}
```
### GenericRecordBuilder.compact

A single method will be added to the existing GenericRecordBuilder class 

```
@Beta
public interface GenericRecord {

    //........   
    interface Builder {
        //........    
    
        /**
         * @return a new constructed GenericRecord to be serialized in new format
         */
        @Nonnull
        static Builder compact(String className) {
            //......
            return compactBuilder;
        }

    }
}

```

### GenericRecord.getFields

A single method will be added to the existing GenericRecord class.

```
@Beta
public interface GenericRecord {

    Set<String> getFieldNames();
}

```

### FieldType

We will reuse FieldType because it is exposed from `GenericRecord.getFieldType` as a common type descriptor for all formats
that implements `GenericRecord` . We are reusing `FieldType` because we want to make the return value of
`ClassDefinition.getField` consistent with `GenericRecord.getFieldType` . 
TODO: re-evaluate this decision. May be we should not care about Portable(ClassDefinition API) anymore since it will be deprecated.
See existing [FieldType implementation](https://github.com/hazelcast/hazelcast/blob/4.1/hazelcast/src/main/java/com/hazelcast/nio/serialization/FieldType.java)  to compare. 

```

public enum FieldType {
    PORTABLE(0, false, MAX_VALUE), //this value has no meaning for new format. It will never be used for the new format.
    BYTE(1, true, BYTE_SIZE_IN_BYTES),
    BOOLEAN(2, true, BOOLEAN_SIZE_IN_BYTES),
......
    PORTABLE_ARRAY(10, false, MAX_VALUE),//this value has no meaning for new format. It will never be used for the new format.
......
    OBJECT(32, false, MAX_VALUE),         //two additional enums to replace PORTABLE and PORTABLE_ARRAY in the new format.
    OBJECT_ARRAY(33, false, MAX_VALUE);   //similarly these two have no meaning for PORTABLE. 
}
```

### CompactSerializationConfig

#### Programmatic
The CompactSerializationConfig will be accessed from SerializationConfig as follows o both client and member API.
```
//get method
CompactSerializationConfig compactSerializationConfig = config.getSerializationConfig().getCompactSerializationConfig();
//set method
config.getSerializationConfig().setCompactSerializationConfig(...);
```

```
public class CompactSerializationConfig {
    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     * <p>
     * class name is determined automatically from clazz. It is full path including package by default
     * fields are determined automatically from class via reflection
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz) {
    }

    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     * <p>
     * fields are determined automatically from class via reflection
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz, String alias) {
    }

    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     * class name is determined automatically from clazz. It is full path including package by default
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz, CompactSerializer<T> explicitSerializer) {
    }

    /**
     * Register class to be serialized via compact serializer.
     * Overrides Portable,Identified,Java Serializable or GlobalSerializer
     *
     * @param clazz Class to be serialized via compact serializer
     */
    public <T> void register(Class<T> clazz, String alias, CompactSerializer<T> explicitSerializer) {
    }
}
```

#### XML

Note that XML and Yaml configuration is for the client-side and only when interoperability between different languages is needed. 
It could also be used for the embedded member use-case,but not needed on the cluster when applications are accessing from the client. 
Declarative configuration is alternative of the programming configuration that is used in the use case examples on this document.
See the use cases examples for more details about when the explicit configuration is needed.

```
 <serialization>
    <compact-serializer>
        <classes>
            <class name="com.sample.domainClass.Employee"></class>
            <class name="com.sample.domainClass.Person">
                <alias>person</alias>
            </class>
            <class name="domainClass.Intern">
                <alias>employee</alias>
                <serializer>com.sample.serializers.InternSerializer</serializer>
            </class>
        </classes>
    </compact-serializer>        
</serialization>
```

Note that com.sample.serializers.InternSerializer should be instance of `CompactSerializer`

#### YAML

Note that XML and Yaml configuration is for the client-side and only when interoperability between different languages is needed. 
It could also be used for the embedded member use-case,but not needed on the cluster when applications are accessing from the client. 
Declarative configuration is alternative of the programming configuration that is used in the use case examples on this document.
See the use cases examples for more details about when the explicit configuration is needed.

```
serialization:
    compact-serializer:
      classes:
        com.sample.domainClass.Employee:
        com.sample.domainClass.Person:
          alias: person
        com.sample.domainClass.Intern:
          alias: employee
          serializer: com.sample.serializers.InternSerializer
```
Note that com.sample.serializers.InternSerializer should be instance of `CompactSerializer`
