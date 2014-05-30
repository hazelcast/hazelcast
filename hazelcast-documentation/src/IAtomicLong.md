

## IAtomicLong


Hazelcast IAtomicLong is the distributed implementation of  `java.util.concurrent.atomic.AtomicLong`. It offers most of AtomicLong's operations such as `get`, `set`, `getAndSet`, `compareAndSet` and `incrementAndGet`. Since IAtomicLong is a distributed implementation, these operations involve remote calls and hence their performances differ from that of AtomicLong.
Below sample code creates an instance, increments it by a million and prints the count.
```javapublic class Member {   public static void main(String[] args) {    HazelcastInstance hz = Hazelcast.newHazelcastInstance(); 	
	IAtomicLong counter = hz.getAtomicLong("counter");	for (int k = 0; k < 1000 * 1000; k++) {
		if (k % 500000 == 0){ System.out.println("At: "+k);		}		counter.incrementAndGet();
	}	System.out.printf("Count is %s\n", counter.get());	System.exit(0); }}
```When you start other instances with the code above, you will see the count as *member count * a million*.
You can send functions to an IAtomicLong. `Function` is a Hazelcast owned, single method interface. Below sample `Function` implementation doubles the original value.
```javaprivate static class Add2Function implements Function <Long ,Long > { 
	@Override	public Long apply(Long input) { 
		return input +2;	}
}```Below methods can be used to execute functions on IAtomicLong.
-	`apply`: It applies the function to the value in IAtomicLong without changing the actual value and returning the result.-	`alter`: It alters the value stored in the IAtomicLong by applying the function. It will not send back a result.
-	`alterAndGet`: It alters the value stored in the IAtomicLong by applying the function, storing the result in the IAtomicLong and returning the result.-	`getAndAlter`: It alters the value stored in the IAtomicLong by applying the function and returning the original value.Below sample code includes these methods.


```java
public class Member {	public static void main(String[] args) {		HazelcastInstance hz = Hazelcast.newHazelcastInstance(); 		
		IAtomicLong atomicLong = hz.getAtomicLong("counter");		atomicLong.set(1);		long result = atomicLong.apply(new Add2Function()); 		
		System.out.println("apply.result:" + result); 		
		System.out.println("apply.value:" + atomicLong.get());		atomicLong.set(1);		atomicLong.alter(new Add2Function()); 			
		System.out.println("alter.value:"+atomicLong.get());		atomicLong.set(1);		result = atomicLong.alterAndGet(new Add2Function()); 		
		System.out.println("alterAndGet.result:" + result); 		
		System.out.println("alterAndGet.value:"+atomicLong.get());		atomicLong.set(1);		result = atomicLong.getAndAlter(new Add2Function()); 		
		System.out.println("getAndAlter.result:"+result); 		
		System.out.println("getAndAlter.value:"+atomicLong.get());		System.exit(0);
	}}
```The reason for using a function instead of a simple code line like `atomicLong.set(atomicLong.get()+2));` is that read and write operations of IAtomicLong are not atomic. Since it is a distributed implementation, those operations can be remote ones, which may lead to race problems. By using functions, the data is not pulled in to the code, but the code is sent to the data. And this makes it more scalable.<font color="red">***Note:***</font> *IAtomicLong has 1 synchronous backup and no asynchronous backups. Its backup count is not configurable.*
<br></br>