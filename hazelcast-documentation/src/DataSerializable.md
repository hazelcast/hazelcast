
## Data Serializable

For a faster serialization of objects, Hazelcast recommends to implement `com.hazelcast.nio.serialization.IdentifiedDataSerializable` which is slightly better version of `com.hazelcast.nio.serialization.DataSerializable`.

Here is an example of a class implementing `com.hazelcast.nio.serialization.DataSerializable` interface.

```java
public class Address implements com.hazelcast.nio.serialization.DataSerializable {
    private String street;
    private int zipCode;
    private String city;
    private String state;

    public Address() {}

    //getters setters..

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(street);
        out.writeInt(zipCode);
        out.writeUTF(city);
        out.writeUTF(state);
    }

    public void readData(ObjectDataInput in) throws IOException {
        street    = in.readUTF();
        zipCode = in.readInt();
        city    = in.readUTF();
        state    = in.readUTF();
    }
}
```

Let's take a look at another example which is encapsulating a `DataSerializable` field.


```java
public class Employee implements com.hazelcast.nio.serialization.DataSerializable {
    private String firstName;
    private String lastName;
    private int age;
    private double salary;
    private Address address; //address itself is DataSerializable

    public Employee() {}

    //getters setters..

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(firstName);
        out.writeUTF(lastName);
        out.writeInt(age);
        out.writeDouble (salary);
        address.writeData (out);
    }

    public void readData (ObjectDataInput in) throws IOException {
        firstName = in.readUTF();
        lastName  = in.readUTF();
        age       = in.readInt();
        salary       = in.readDouble();
        address   = new Address();
        // since Address is DataSerializable let it read its own internal state
        address.readData (in);
    }
}
```

As you can see, since `address` field itself is `DataSerializable`, it is calling `address.writeData(out)` when writing and `address.readData(in)` when reading. Also note that, the order of writing and reading fields should be the same. While Hazelcast serializes a DataSerializable, it writes the `className` first and when de-serializes it, `className` is used to instantiate the object using reflection. 

**IdentifiedDataSerializable** 

To avoid the reflection and long class names, `IdentifiedDataSerializable` can be used instead of `DataSerializable`. Note that, `IdentifiedDataSerializable` extends `DataSerializable` and introduces two new methods.

-   `int getId();`
-   `int getFactoryId();`


`IdentifiedDataSerializable` uses `getId()` instead of className and uses getFactoryId() to load the class given the Id. To complete the implementation, a `com.hazelcast.nio.serialization.DataSerializableFactory` should also be implemented and registered into `SerializationConfig` which can be accessed from `Config.getSerializationConfig()`. Factory's responsibility is to return an instance of the right `IdentifiedDataSerializable` object, given the Id. So far this is the most efficient way of Serialization that Hazelcast supports of the shelf.
