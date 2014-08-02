


## Portable

As an alternative to the existing serialization methods, Hazelcast offers a language/platform independent Portable serialization that have the following advantages:

-   Support multi-version of the same object type.
-   Fetching individual fields without having to rely on reflection.
-   Querying and indexing support without de-serialization and/or reflection.

In order to support these features, a serialized Portable object is offered containing meta information like the version and the concrete location of the each field in the binary data. This way Hazelcast is able to navigate in the `byte[]` and de-serialize only the required field without actually de-serializing the whole object which improves the Query performance.

With multi-version support, you can have two nodes where each of them having different versions of the same object and Hazelcast will store both meta information and use the correct one to serialize and de-serialize Portable objects depending on the node. This is very helpful when you are doing a rolling upgrade without shutting down the cluster.

Also note that, Portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.

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

Similar to `IdentifiedDataSerializable`, a Portable Class must provide `classId` and`factoryId`. The Factory object will be used to create the Portable object given the `classId`.

A sample `Factory` could be implemented as following:

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

The last step is to register the `Factory` to the `SerializationConfig`. Below are the programmatic and declarative configurations for this step in order.


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


Note that the `id` that is passed to the `SerializationConfig` is same as the `factoryId` that `Foo` class returns.


### Versions

More than one versions of the same class may need to be serialized and deserialized.  For example, a client may have an older version of a class, and the node to which it is connected can have a newer version of the same class. 

Portable serialization supports versioning. Version can be provided declaratively in the configuration file `hazelcast.xml` using the `portable-version` tag, as shown below.

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

Things to consider related to versioning is given below:

- It is important to change the version whenever an update is performed in the serialized fields of a class (e.g. increment the version).
- Assume that, for example, a client performs a Portable deserialization on a field. If that Portable is updated by removing that field on the cluster side, this may lead to a problem.
- Portable serialization does not use reflection and hence, fields in the class and in the serialized content are not automatically mapped. So, field renaming is a simpler process. Also, since the class ID is stored, renaming the Portable does not lead to problems.
- Types of fields need to be updated carefully. Yet, Hazelcast performs basic type upgradings (e.g. `int` to `float`).

### Null Portable Serialization

One needs to be careful when serializing null portables. Hazelcast lazily creates a class definition of portable internally
when the user first serializes. This class definition is stored and used later for deserializing that portable class. When
the user tries to serialize a null portable and if there is no class definition at the moment, Hazelcast throws an
exception saying that `com.hazelcast.nio.serialization.HazelcastSerializationException: Cannot write null portable
without explicitly registering class definition!`. 

There are two solutions to get rid of this exception. First way, the user can put
a non-null portable class of the same type before any other operation. Second way, a class definition can be registered manually
in serialization config as shown below.

```java
Config config = new Config();
final ClassDefinition classDefinition = new ClassDefinitionBuilder(Foo.factoryId, Foo.getClassId)
                       .addUTFField("foo").build();
config.getSerializationConfig().addClassDefinition(classDefinition);
Hazelcast.newHazelcastInstance(config);
```


### DistributedObject Serialization

Putting a DistributedObject (e.g. Hazelcast Semaphore, Queue, etc.) in a machine and getting it from another one is not a straightforward operation. For this case, passing the ID and type of the DistributedObject can be a solution. For deserialization, try to get the object from HazelcastInstance. For instance, if your distributed object is an instance of IQueue, you can either use `HazelcastInstance.getQueue(id)` or `Hazelcast.getDistributedObject`.

Also, `HazelcastInstanceAware` interface can be used in the case of a deserialization of a Portable DistributedObject and if it gets an ID to be looked up. HazelcastInstance is set after deserialization and hence, you first need to store the ID and then retrieve the DistributedObject using `setHazelcastInstance` method. 
 


