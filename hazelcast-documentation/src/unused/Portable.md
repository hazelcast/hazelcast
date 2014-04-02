
## Portable Serialization

As an alternative to the existing serialization methods, Hazelcast offers a Portable serialization that have the following advantages:

-   Support multiversion of the same object type.
-   Fetching individual fields without having to rely on reflection.
-   Querying and indexing support without de-serialization and/or reflection.

In order to support these features, a serialized Portable object is offered containing meta information like the version and the concrete location of the each field in the binary data. This way Hazelcast is able to navigate in the byte[] and de-serialize only the required field without actually de-serializing the whole object which improves the Query performance.

With multiversion support, you can have two nodes where each of them having different versions of the same object and Hazelcast will store both meta information and use the correct one to serialize and de-serialize Portable objects depending on the node. This is very helpful when you are doing a rolling upgrade without shutting down the cluster.

Also note that, Portable serialization is totally language independent and is used as the binary protocol between Hazelcast server and clients.

A sample Portable implementation of a Foo class would look like the following.

```java
public class Foo implements Portable{
    final static int ID = 5;

    private String foo;

    public String getFoo() {
        return foo;
    }

    public void setFoo(String foo) {
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
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("foo", foo);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        foo = reader.readUTF("foo");
    }
}        
```

Similar to `IdentifiedDataSerializable`, a Portable Class must provide `classId` and`factoryId`. The Factory object will be used to create the Portable object given the `classId`.

A sample `Factory` could be implemented as following:

```java
public class MyPortableFactory implements PortableFactory {

    @Override
    public Portable create(int classId) {
        if (Foo.ID == classId)
            return new Foo();
        else return null;
     }
}            
```

The last step is to register the `Factory` to the `SerializationConfig`.

-	**Programmatic Configuration**

	```java
Config config = new Config();
config.getSerializationConfig().addPortableFactory(1, new MyPortableFactory());
                ```

-	**XML Configuration**

	```xml
<hazelcast>
    <serialization>
        <portable-version>0</portable-version>
        <portable-factories>
            <portable-factory factory-id="1">com.hazelcast.nio.serialization.MyPortableFactory</portable-factory>
        </portable-factories>
    </serialization>
</hazelcast>               
```

Note that the `id` that is passed to the `SerializationConfig` is same as the `factoryId` that `Foo` class returns.
