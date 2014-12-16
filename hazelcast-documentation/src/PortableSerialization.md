


## Portable

As an alternative to the existing serialization methods, Hazelcast offers a language/platform independent Portable serialization that has the following advantages:

-   Supports multi-version of the same object type.
-   Fetches individual fields without having to rely on reflection.
-   Queries and indexing support without de-serialization and/or reflection.

In order to support these features, a serialized Portable object contains meta information like the version and the concrete location of the each field in the binary data. This way, Hazelcast navigates in the `byte[]` and de-serializes only the required field without actually de-serializing the whole object. This improves the Query performance.

With multi-version support, you can have two nodes where each of them have different versions of the same object. Hazelcast will store both meta information and use the correct one to serialize and de-serialize Portable objects depending on the node. This is very helpful when you are doing a rolling upgrade without shutting down the cluster.

Portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.

A sample Portable implementation of a Foo class would look like the following.

```java
public class Foo implements Portable{
  final static int ID = 5;

  private String foo;

  public String getFoo() {
    return foo;
  }

  public void setFoo( String foo ) {
    this.foo = foo;
  }

  @Override
  public int getFactoryId() {
    return 1;
  }

  @Override
  public int getClassId() {
    return ID;
  }

  @Override
  public void writePortable( PortableWriter writer ) throws IOException {
    writer.writeUTF( "foo", foo );
  }

  @Override
  public void readPortable( PortableReader reader ) throws IOException {
    foo = reader.readUTF( "foo" );
  }
}        
```

Similar to `IdentifiedDataSerializable`, a Portable Class must provide `classId` and`factoryId`. The Factory object will create the Portable object given the `classId`.

An example `Factory` could be implemented as following:

```java
public class MyPortableFactory implements PortableFactory {

  @Override
  public Portable create( int classId ) {
    if ( Foo.ID == classId )
      return new Foo();
    else
      return null;
  }
}            
```

The last step is to register the `Factory` to the `SerializationConfig`. Below are the programmatic and declarative configurations for this step.


```java
Config config = new Config();
config.getSerializationConfig().addPortableFactory( 1, new MyPortableFactory() );
```


```xml
<hazelcast>
  <serialization>
    <portable-version>0</portable-version>
    <portable-factories>
      <portable-factory factory-id="1">
          com.hazelcast.nio.serialization.MyPortableFactory
      </portable-factory>
    </portable-factories>
  </serialization>
</hazelcast>
```


Note that the `id` that is passed to the `SerializationConfig` is the same as the `factoryId` that the `Foo` class returns.


### Versions

More than one version of the same class may need to be serialized and deserialized.  For example, a client may have an older version of a class, and the node to which it is connected can have a newer version of the same class. 

Portable serialization supports versioning. You can declare Version in the configuration file `hazelcast.xml` using the `portable-version` element, as shown below.

```xml
<serialization>
  <portable-version>1</portable-version>
  <portable-factories>
    <portable-factory factory-id="1">
        PortableFactoryImpl
    </portable-factory>
  </portable-factories>
</serialization>
```

You should consider the following when you perform versioning.

- It is important to change the version whenever an update is performed in the serialized fields of a class (e.g. increment the version).
- If a client performs a Portable deserialization on a field, and then that Portable is updated by removing that field on the cluster side, this may lead to a problem.
- Portable serialization does not use reflection and hence, fields in the class and in the serialized content are not automatically mapped. Field renaming is a simpler process. Also, since the class ID is stored, renaming the Portable does not lead to problems.
- Types of fields need to be updated carefully. Hazelcast performs basic type upgradings (e.g. `int` to `float`).

### Null Portable Serialization

Be careful when serializing null portables. Hazelcast lazily creates a class definition of portable internally
when the user first serializes. This class definition is stored and used later for deserializing that portable class. When
the user tries to serialize a null portable when there is no class definition at the moment, Hazelcast throws an
exception saying that `com.hazelcast.nio.serialization.HazelcastSerializationException: Cannot write null portable
without explicitly registering class definition!`. 

There are two solutions to get rid of this exception. Either put
a non-null portable class of the same type before any other operation, or manually register a class definition in serialization configuration as shown below.

```java
Config config = new Config();
final ClassDefinition classDefinition = new ClassDefinitionBuilder(Foo.factoryId, Foo.getClassId)
                       .addUTFField("foo").build();
config.getSerializationConfig().addClassDefinition(classDefinition);
Hazelcast.newHazelcastInstance(config);
```


### DistributedObject Serialization

Putting a DistributedObject (e.g. Hazelcast Semaphore, Queue, etc.) in a machine and getting it from another one is not a straightforward operation. Passing the ID and type of the DistributedObject can be a solution. For deserialization, you can get the object from HazelcastInstance. For instance, if your distributed object is an instance of `IQueue`, you can either use `HazelcastInstance.getQueue(id)` or `Hazelcast.getDistributedObject`.

You can use the `HazelcastInstanceAware` interface in the case of a deserialization of a Portable DistributedObject if it gets an ID to be looked up. HazelcastInstance is set after deserialization, so you first need to store the ID and then retrieve the DistributedObject using the `setHazelcastInstance` method. 


<br></br>

***RELATED INFORMATION***


*Please refer to the [Serialization Configuration section](#serialization-configuration) for a full description of Hazelcast Serialization configuration.*

 


