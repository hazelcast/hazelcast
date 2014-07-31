



## Serializable & Externalizable

Often, a class needs to implement `java.io.Serializable` interface and therefore native Java serialization is the easiest way for serialization. Let's take a look at the sample code below.

```java
public class Employee implements Serializable { 
  private static final long serialVersionUID = 1L;
  private String surname;
  
  public Employee( String surname ) { 
    this.surname = surname;
  } 
}
```

Here, the fields that are non-static and non-transient are automatically serialized. To eliminate class compatibility issues, it is recommended to add a `serialVersionUID`, as shown above. Also, when you are using methods that perform byte-content comparisons (e.g. `IMap.replace()`) and if byte-content of equal objects is different, you may face with unexpected behaviors. So, if the class relies on, for example a hash map, `replace` method may fail. The reason for this is the hash map, being a serialized data structure with unreliable byte-content.

Hazelcast also supports `java.io.Externalizable`. This interface offers more control on the way how the fields are serialized or deserialized. Compared to native Java serialization, it also can have a positive effect on the performance. There is no need for a `serialVersionUID` Let's take a look the sample code below.

```java
public class Employee implements Externalizable { 
  private String surname;
  public Employee(String surname) { 
        this.surname = surname;
  }
  
  @Override
  public void readExternal( ObjectInput in )
      throws IOException, ClassNotFoundException {
    this.surname = in.readUTF();
  }
    
  @Override
  public void writeExternal( ObjectOutput out )
      throws IOException {
    out.writeUTF(surname); 
  }
}
```

As can be seen, writing and reading of fields are written explicitly. Note that, reading should be performed in the same order as writing.

