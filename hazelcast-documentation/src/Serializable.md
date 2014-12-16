



## Serializable & Externalizable

A class often needs to implement the `java.io.Serializable` interface; native Java serialization is the easiest way to do serialization. Let's take a look at the example code below.

```java
public class Employee implements Serializable { 
  private static final long serialVersionUID = 1L;
  private String surname;
  
  public Employee( String surname ) { 
    this.surname = surname;
  } 
}
```

Here, the fields that are non-static and non-transient are automatically serialized. To eliminate class compatibility issues, it is recommended that you add a `serialVersionUID`, as shown above. Also, when you are using methods that perform byte-content comparisons (e.g. `IMap.replace()`) and if byte-content of equal objects is different, you may face unexpected behaviors. Therefore, if the class relies on, for example, a hash map, `replace` method may fail. The reason for this is the hash map is a serialized data structure with unreliable byte-content.

Hazelcast also supports `java.io.Externalizable`. This interface offers more control on the way fields are serialized or deserialized. Compared to native Java serialization, it also can have a positive effect on performance. With `java.io.Externalizable`, there is no need to add `serialVersionUID`.

Let's take a look at the example code below.

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

Writing and reading of fields are performed explicitly. Note that reading should be performed in the same order as writing.

