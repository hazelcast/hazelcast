



## DataSerializable

As mentioned in the [Serializable & Externalizable section](#serializable-externalizable), Java serialization is an easy mechanism. However, we do not have a control on how fields are serialized or deserialized. Moreover, this mechanism can lead to excessive CPU loads since it keeps track of objects to handle the cycles and streams class descriptors. These are performance decreasing factors; thus, serialized data may not have an optimal size.

The `DataSerializable` interface of Hazelcast overcomes these issues. Here is an example of a class implementing the `com.hazelcast.nio.serialization.DataSerializable` interface.

```java
public class Address implements DataSerializable {
  private String street;
  private int zipCode;
  private String city;
  private String state;

  public Address() {}

  //getters setters..

  public void writeData( ObjectDataOutput out ) throws IOException {
    out.writeUTF(street);
    out.writeInt(zipCode);
    out.writeUTF(city);
    out.writeUTF(state);
  }

  public void readData( ObjectDataInput in ) throws IOException {
    street = in.readUTF();
    zipCode = in.readInt();
    city = in.readUTF();
    state = in.readUTF();
  }
}
```

Let's take a look at another example which encapsulates a `DataSerializable` field.


```java
public class Employee implements DataSerializable {
  private String firstName;
  private String lastName;
  private int age;
  private double salary;
  private Address address; //address itself is DataSerializable

  public Employee() {}

  //getters setters..

  public void writeData( ObjectDataOutput out ) throws IOException {
    out.writeUTF(firstName);
    out.writeUTF(lastName);
    out.writeInt(age);
    out.writeDouble (salary);
    address.writeData (out);
  }

  public void readData( ObjectDataInput in ) throws IOException {
    firstName = in.readUTF();
    lastName = in.readUTF();
    age = in.readInt();
    salary = in.readDouble();
    address = new Address();
    // since Address is DataSerializable let it read its own internal state
    address.readData(in);
  }
}
```

As you can see, since `address` field itself is `DataSerializable`, it is calling `address.writeData(out)` when writing and `address.readData(in)` when reading. Also note that, the order of writing and reading fields should be the same. While Hazelcast serializes a `DataSerializable`, it writes the `className` first. When Hazelcast de-serializes it, `className` is used to instantiate the object using reflection.

![image](images/NoteSmall.jpg) ***NOTE:*** *Since Hazelcast needs to create an instance during deserialization,`DataSerializable` class has a no-arg constructor.*

![image](images/NoteSmall.jpg) ***NOTE:*** *`DataSerializable` is a good option if serialization is only needed for in-cluster communication.*


### IdentifiedDataSerializable

For a faster serialization of objects, avoiding reflection and long class names, Hazelcast recommends you implement `com.hazelcast.nio.serialization.IdentifiedDataSerializable` which is a slightly better version of `DataSerializable`.

`DataSerializable` uses reflection to create a class instance, as mentioned in the [DataSerializable section](#dataserializable). But, `IdentifiedDataSerializable` uses a factory for this purpose and it is faster during deserialization which requires new instance creations.

`IdentifiedDataSerializable` extends `DataSerializable` and introduces two new methods.

-   `int getId();`
-   `int getFactoryId();`


`IdentifiedDataSerializable` uses `getId()` instead of class name, and it uses `getFactoryId()` to load the class when given the Id. To complete the implementation, `com.hazelcast.nio.serialization.DataSerializableFactory` should also be implemented and registered into `SerializationConfig` which can be accessed from `Config.getSerializationConfig()`. Factory's responsibility is to return an instance of the right `IdentifiedDataSerializable` object, given the Id. So far this is the most efficient way of Serialization that Hazelcast supports off the shelf.

Let's take a look at the example code below and configuration to see `IdentifiedDataSerializable` in action.

```java
public class Employee
    implements IdentifiedDataSerializable {
     
  private String surname;
  
  public Employee() {}
  
  public Employee( String surname ) { 
    this.surname = surname;
  }
  
  @Override
  public void readData( ObjectDataInput in ) 
      throws IOException {
    this.surname = in.readUTF();
  }
  
  @Override
  public void writeData( ObjectDataOutput out )
      throws IOException { 
    out.writeUTF( surname );
  }
  
  @Override
  public int getFactoryId() { 
    return EmployeeDataSerializableFactory.FACTORY_ID;
  }
  
  @Override
  public int getId() { 
    return EmployeeDataSerializableFactory.EMPLOYEE_TYPE;
  }
   
  @Override
  public String toString() {
    return String.format( "Employee(surname=%s)", surname ); 
  }
}
```
 
The methods `getId` and `getFactoryId` return a unique positive number within the `EmployeeDataSerializableFactory`. Now, let's create an instance of this `EmployeeDataSerializableFactory`.

```java
public class EmployeeDataSerializableFactory 
    implements DataSerializableFactory{
   
  public static final int FACTORY_ID = 1;
   
  public static final int EMPLOYEE_TYPE = 1;

  @Override
  public IdentifiedDataSerializable create(int typeId) {
    if ( typeId == EMPLOYEE_TYPE ) { 
      return new Employee();
    } else {
      return null; 
    }
  }
}
```

The only method that should be implemented is `create`, as seen in the above example. It is recommended that you use a `switch`-``case` statement instead of multiple `if`-`else` blocks if you have a lot of subclasses. Hazelcast throws an exception if null is returned for `typeId`.

As the last step, you need to register `EmployeeDataSerializableFactory` declaratively (declare in the configuration file `hazelcast.xml`) as shown below. Note that `factory-id` has the same value of `FACTORY_ID` in the above code. This is crucial to enable Hazelcast to find the correct factory.

```xml
<hazelcast> 
  ...
  <serialization>
    <data-serializable-factories>
      <data-serializable-factory
        factory-id="1">EmployeeDataSerializableFactory
      </data-serializable-factory>
    </data-serializable-factories>
  </serialization>
  ...
</hazelcast>
```


<br></br>

***RELATED INFORMATION***


*Please refer to the [Serialization Configuration section](#serialization-configuration) for a full description of Hazelcast Serialization configuration.*

 