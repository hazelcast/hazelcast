


### ByteArraySerializer

`ByteArraySerializer` exposes the raw ByteArray used internally by Hazelcast. It is a good option if the serialization library you are using deals with ByteArrays instead of streams.

Let's implement `ByteArraySerializer` for the `Employee` class mentioned in the [StreamSerializer section](#streamserializer).

```java
public class EmployeeByteArraySerializer
    implements ByteArraySerializer<Employee> {

  @Override
  public void destroy () { 
  }

  @Override
  public int getTypeId () {
    return 1; 
  }

  @Override
  public byte[] write( Employee object )
      throws IOException { 
    return object.getName().getBytes();
  }

  @Override
  public Employee read( byte[] buffer ) 
      throws IOException { 
    String surname = new String( buffer );
    return new Employee( surname );
  }
}
```

As usual, let's register the `EmployeeByteArraySerializer` in the configuration file `hazelcast.xml`, as shown below.

```xml
<serialization>
  <serializers>
    <serializer type-class="Employee">EmployeeByteArraySerializer</serializer>
  </serializers>
</serialization>
```


<br></br>

***RELATED INFORMATION***


*Please refer to the [Serialization Configuration section](#serialization-configuration) for a full description of Hazelcast Serialization configuration.*

 