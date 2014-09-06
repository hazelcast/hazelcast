

## IAtomicReference

The IAtomicLong is very useful if you need to deal with a long, but in some cases you need to deal with a reference. That is why Hazelcast also supports the IAtomicReference which is the distributed version of the `java.util.concurrent.atomic.AtomicReference`.

So lets see the IAtomicReference in action:

```java
public class Member {
    public static void main(String[] args) {
        Config config = new Config();

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        IAtomicReference<String> ref = hz.getAtomicReference("reference");
        ref.set("foo");
        System.out.println(ref.get());
        System.exit(0);
    } 
}
```

When you execute the above sample, you will see the below output.

`foo`

Just like the IAtomicLong, the IAtomicReference has a bunch of methods that accept a 'function' as argument like `alter`, `alterAndGet`, `getAndAlter` and `apply`. There are two big advantages of using these methods:

-	It is better to send the function to the data, then the data to the function from a performance point of view. Often the function is a lot smaller than the value and therefore cheaper to send over the line. Also the function only needs to be transferred once to the target machine, and the value needs to be transferred twice.
-	You do not need to deal with concurrency control. If you would perform a load, transform, store, you could run into a data race since another thread might have updated the value you are about to overwrite. 

Of course there are some issues you need to be aware of as described below.

-	IAtomicReference works based on the byte-content and not on the object-reference. So if you are using the `compareAndSet` method, it is important not to change to original value because its serialized content will then be different. 
Also important to know is that, if you rely on Java serialization, sometimes (especially with hashmaps) the same object can result in different binary content.
-	IAtomicReference will always have 1 synchronous backup.
-	All methods returning an object will return a private copy. So you can modify it, but the rest of the world will be shielded from your changes. If you want these changes to be visible to the rest of the world, you need to write the change back to the IAtomicReference; but be careful with introducing a data-race. 
-	The 'in memory format' of an IAtomicReference is `binary`. So the receiving side does not need to have the class definition available, unless it needs to be deserialized on the other side (e.g. because a method like 'alter' is executed. This deserialization is done for every call that needs to have the object instead of the binary content, so be careful with expensive object graphs that need to be deserialized.
-	If you have an object with many fields or an object graph, and you only need to calculate some information or need a subset of fields, you can use the `apply` method for that. This way the whole object does not need to be sent over the line, only the information that is relevant.

<br></br>