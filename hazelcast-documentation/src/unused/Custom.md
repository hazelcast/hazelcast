## Custom Serialization

Hazelcast lets you plug a custom serializer to be used for serialization of objects.

Assume that you have a class
Foo
and you would like to customize the serialization. The reasons could be
Foo
is not Serializable or you are not happy with the default serialization.

```java
public class Foo {
    private String foo;
    public String getFoo() {
        return foo;
    }
    public void setFoo(String foo) {
        this.foo = foo;
    }
}
```

Assume that our custom serialization will serialize
Foo
into XML. First we need to implement a
`com.hazelcast.nio.serialization.StreamSerializer`. A very simple one that uses XMLEncoder and XMLDecoder, would look like the following:

```java
public static class FooXmlSerializer implements StreamSerializer<Foo> {

    @Override
    public int getTypeId() {
        return 10;
    }

    @Override
    public void write(ObjectDataOutput out, Foo object) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        XMLEncoder encoder = new XMLEncoder(bos);
        encoder.writeObject(object);
        encoder.close();
        out.write(bos.toByteArray());
    }

    @Override
    public Foo read(ObjectDataInput in) throws IOException {
        final InputStream inputStream = (InputStream) in;
        XMLDecoder decoder = new XMLDecoder(inputStream);
        return (Foo) decoder.readObject();
    }

    @Override
    public void destroy() {
    }
}
```

Note that
`typeId`
must be unique as Hazelcast will use it to lookup the StreamSerializer while it de-serializes the object. Now, the last required step is to register the StreamSerializer to the Configuration.


Programmatic Configuration

```java
SerializerConfig sc = new SerializerConfig().
        setImplementation(new FooXmlSerializer()).
        setTypeClass(Foo.class);
Config config = new Config();
config.getSerializationConfig().addSerializerConfig(sc);
```
XML Configuration

```xml
<hazelcast>
    <serialization>
        <serializers>
            <serializer type-class="com.www.Foo">com.www.FooXmlSerializer</serializer>
        </serializers>
    </serialization>
</hazelcast>
```
From now on, Hazelcast will use
`FooXmlSerializer`
to serialize Foo objects. This way one can write an adapter (
StreamSerializer
) for any Serialization framework and plug it into Hazelcast.
