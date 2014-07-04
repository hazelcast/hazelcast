# Performance




## Data Affinity

**Co-location of related data and computation**

Hazelcast has a standard way of finding out which member owns/manages each key object. Following operations will be routed to the same member, since all of them are operating based on the same key, "key1".

```java    
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Map mapa = hazelcastInstance.getMap( "mapa" );
Map mapb = hazelcastInstance.getMap( "mapb" );
Map mapc = hazelcastInstance.getMap( "mapc" );
mapa.put( "key1", value );
mapb.get( "key1" );
mapc.remove( "key1" );
// since map names are different, operation will be manipulating
// different entries, but the operation will take place on the
// same member since the keys ("key1") are the same

hazelcastInstance.getLock( "key1" ).lock();
// lock operation will still execute on the same member of the cluster
// since the key ("key1") is same

hazelcastInstance.getExecutorService().executeOnKeyOwner( runnable, "key1" );
// distributed execution will execute the 'runnable' on the same member
// since "key1" is passed as the key.   
```

So, when the keys are the same, then entries are stored on the same node. But we sometimes want to have related entries stored on the same node. Consider customer and his/her order entries. We would have customers map with customerId as the key and orders map with orderId as the key. Since customerId and orderIds are different keys, customer and his/her orders may fall into different members/nodes in your cluster. So how can we have them stored on the same node? The trick here is to create an affinity between customer and orders. If we can somehow make them part of the same partition then these entries will be co-located. We achieve this by making orderIds `PartitionAware`.

```java
public class OrderKey implements Serializable, PartitionAware {
  int customerId;
  int orderId;

  public OrderKey( int orderId, int customerId ) {
    this.customerId = customerId;
    this.orderId = orderId;
  }

  public int getCustomerId() {
    return customerId;
  }

  public int getOrderId() {
    return orderId;
  }

  public Object getPartitionKey() {
    return customerId;
  }

  @Override
  public String toString() {
    return "OrderKey{" +
        "customerId=" + customerId +
        ", orderId=" + orderId +
      '}';
    }
}
```

Notice that OrderKey implements `PartitionAware` and `getPartitionKey()` returns the `customerId`. This will make sure that `Customer` entry and its `Order`s are going to be stored on the same node.

```java
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
Map mapCustomers = hazelcastInstance.getMap( "customers" )
Map mapOrders = hazelcastInstance.getMap( "orders" )
// create the customer entry with customer id = 1
mapCustomers.put( 1, customer );
// now create the orders for this customer
mapOrders.put( new OrderKey( 21, 1 ), order );
mapOrders.put( new OrderKey( 22, 1 ), order );
mapOrders.put( new OrderKey( 23, 1 ), order );
```


Assume that you have a customers map where `customerId` is the key and the customer object is the value, customer object contains the customer's orders, and you want to remove one of the orders of a customer and return the number of remaining orders. Here is how you would normally do it:

```java
public static int removeOrder( long customerId, long orderId ) throws Exception {
  IMap<Long, Customer> mapCustomers = instance.getMap( "customers" );
  mapCustomers.lock( customerId );
  Customer customer = mapCustomers.get( customerId );
  customer.removeOrder( orderId );
  mapCustomers.put( customerId, customer );
  mapCustomers.unlock( customerId );
  return customer.getOrderCount();
}
```

There are couple of things you should consider:

1.  There are four distributed operations there: lock, get, put, unlock. Can you reduce the number of distributed operations?

2.  Customer object may not be that big, but can you not have to pass that object through the wire? Notice that, customer object is being passed through the wire twice; get and put.

So instead, why not moving the computation over to the member (JVM) where your customer data actually is. Here is how you can do this with distributed executor service:

1.  Send a `PartitionAware` `Callable` task.

2.  `Callable` does the deletion of the order right there and returns with the remaining order count.

3.  Upon completion of the `Callable` task, return the result (remaining order count). Plus, you do not have to wait until the task is completed; since distributed executions are asynchronous, you can do other things in the meantime.

Here is a sample code:

```java
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

public int removeOrder(long customerId, long orderId) throws Exception {
    IExecutorService es = hazelcastInstance
        .getExecutorService( "ExecutorService" );
    OrderDeletionTask task = new OrderDeletionTask( customerId, orderId );
    Future<Integer> future = es.submit( task );
    int remainingOrders = future.get();
    return remainingOrders;
}

public static class OrderDeletionTask
    implements Callable<Integer>, PartitionAware, Serializable {

    private long customerId;
    private long orderId;

    public OrderDeletionTask() {
    }
    
    public OrderDeletionTask(long customerId, long orderId) {
        super();
        this.customerId = customerId;
        this.orderId = orderId;
    }
    
    public Integer call () {
        IMap<Long, Customer> mapCustomers = hazelcastInstance.getMap( "customers" );
        mapCustomers.lock( customerId );
        Customer customer = mapCustomers.get( customerId );
        customer.removeOrder( orderId );
        mapCustomers.put( customerId, customer );
        mapCustomers.unlock( customerId );
        return customer.getOrderCount();
    }

    public Object getPartitionKey() {
        return customerId;
    }
}
```

Benefits of doing the same operation with distributed `ExecutorService` based on the key are:

-   Only one distributed execution (`es.submit(task)`), instead of four.

-   Less data is sent over the wire.

-   Since lock/update/unlock cycle is done locally (local to the customer data), lock duration for the `Customer` entry is much less, so enabling higher concurrency.


<br> </br>


