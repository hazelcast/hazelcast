
## Custom Serialization

Hazelcast lets you plug a custom serializer for serializing objects. You can use `StreamSerializer` and [`ByteArraySerializer`](#bytearrayserializer) interfaces for this purpose.


### StreamSerializer

You can use a stream to serialize and deserialize data by using `StreamSerializer`. This is a good option for your own implementations. It can also be adapted to external serialization libraries like Kryo, JSON, and protocol buffers.

#### StreamSerializer Example 1

First, let's create a simple object.

```java
public class Employee {
  private String surname;
  
  public Employee( String surname ) {
    this.surname = surname;
  }
}
```

Now, let's implement StreamSerializer for `Employee` class.

```java
public class EmployeeStreamSerializer
    implements StreamSerializer<Employee> {

  @Override
  public int getTypeId () {
    return 1; 
  }

  @Override
  public void write( ObjectDataOutput out, Employee employee )
      throws IOException { 
    out.writeUTF(employee.getSurname());
  }

  @Override
  public Employee read( ObjectDataInput in ) 
      throws IOException { 
    String surname = in.readUTF();
    return new Employee(surname);
  }

  @Override
  public void destroy () { 
  }
}
```

In practice, classes may have many fields. Just make sure the fields are read in the same order as they are written. The type ID must be unique and greater than or equal to **1**. Uniqueness of the type ID enables Hazelcast to determine which serializer will be used during deserialization. 

As the last step, let's register the `EmployeeStreamSerializer` in the configuration file `hazelcast.xml`, as shown below.

```xml
<serialization>
  <serializers>
    <serializer type-class="Employee" class-name="EmployeeStreamSerializer" />
  </serializers>
</serialization>
```
 
![image](images/NoteSmall.jpg) ***NOTE:*** *`StreamSerializer` cannot be created for well-known types (e.g. Long, String) and primitive arrays. Hazelcast already registers these types.*


<br></br>

#### StreamSerializer Example 2

Let's take a look at another example implementing `StreamSerializer`.

```java
public class Foo {
  private String foo;
  
  public String getFoo() {
    return foo;
  }
  
  public void setFoo( String foo ) {
    this.foo = foo;
  }
}
```

Assume that our custom serialization will serialize
Foo into XML. First we need to implement a
`com.hazelcast.nio.serialization.StreamSerializer`. A very simple one that uses XMLEncoder and XMLDecoder could look like the following:

```java
public static class FooXmlSerializer implements StreamSerializer<Foo> {

  @Override
  public int getTypeId() {
    return 10;
  }

  @Override
  public void write( ObjectDataOutput out, Foo object ) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    XMLEncoder encoder = new XMLEncoder( bos );
    encoder.writeObject( object );
    encoder.close();
    out.write( bos.toByteArray() );
  }

  @Override
  public Foo read( ObjectDataInput in ) throws IOException {
    InputStream inputStream = (InputStream) in;
    XMLDecoder decoder = new XMLDecoder( inputStream );
    return (Foo) decoder.readObject();
  }

  @Override
  public void destroy() {
  }
}
```

Note that `typeId` must be unique because Hazelcast will use it to look up the `StreamSerializer` while it de-serializes the object. The last required step is to register the `StreamSerializer` to the Configuration. Below are the programmatic and declarative configurations for this step.

```java
SerializerConfig sc = new SerializerConfig()
    .setImplementation(new FooXmlSerializer())
    .setTypeClass(Foo.class);
Config config = new Config();
config.getSerializationConfig().addSerializerConfig(sc);
```


```xml
<hazelcast>
  <serialization>
    <serializers>
      <serializer type-class="com.www.Foo">com.www.FooXmlSerializer</serializer>
    </serializers>
  </serialization>
</hazelcast>
```

From now on, Hazelcast will use `FooXmlSerializer`
to serialize Foo objects. This way one can write an adapter (StreamSerializer) for any Serialization framework and plug it into Hazelcast.

<br> </br>
