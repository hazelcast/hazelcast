


## HazelcastInstanceAware Interface

You can implement the `HazelcastInstanceAware` interface to access distributed objects for cases where an object is deserialized and needs access to HazelcastInstance.

Let's implement it for the `Employee` class mentioned in the [Custom Serialization](#custom-serialization) section.

```java
public class Employee
    implements Serializable, HazelcastInstanceAware { 
   
  private static final long serialVersionUID = 1L;
  private String surname;
  private transient HazelcastInstance hazelcastInstance;

  public Person( String surname ) { 
    this.surname = surname;
  }

  @Override
  public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
    this.hazelcastInstance = hazelcastInstance;
    System.out.println( "HazelcastInstance set" ); 
  }

  @Override
  public String toString() {
    return String.format( "Person(surname=%s)", surname ); 
  }
}
```


After deserialization, the object is checked if it implements `HazelcastInstanceAware` and the method `setHazelcastInstance` is called. Notice the `hazelcastInstance` is `transient`. This is because this field should not be serialized.

It may be a good practice to inject a HazelcastInstance into a domain object (e.g. `Employee` in the above sample) when used together with `Runnable`/`Callable` implementations. These runnables/callables are executed by `IExecutorService` which sends them to another machine. And after a task is deserialized, run/call method implementations need to access HazelcastInstance.

We recommend you only to set the HazelcastInstance field while using `setHazelcastInstance` method and not to execute operations on the HazelcastInstance. Because, when HazelcastInstance is injected for a `HazelcastInstanceAware` implementation, it may not be up and running at the injection time.




<br></br>