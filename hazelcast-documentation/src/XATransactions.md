 

## XA Transactions

XA describes the interface between the global transaction manager and the local resource manager. XA allows multiple resources (such as databases, application servers, message queues, transactional caches, etc.) to be accessed within the same transaction, thereby preserving the ACID properties across applications. XA uses a two-phase commit to ensure that all resources either commit or rollback any particular transaction consistently (all do the same).

By implementing the `XAResource` interface, Hazelcast provides XA transactions. You can obtain the `XAResource` instance via `TransactionContext`.
Below is example code that uses Atomikos for transaction management.
  
```java
UserTransactionManager tm = new UserTransactionManager();
tm.setTransactionTimeout(60);
tm.begin();

HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
TransactionContext context = hazelcastInstance.newTransactionContext();
XAResource xaResource = context.getXaResource();

Transaction transaction = tm.getTransaction();
transaction.enlistResource(xaResource);
// other resources (database, app server etc...) can be enlisted

try {
  TransactionalMap map = context.getMap("m");
  map.put("key", "value");
  // other resource operations

  tm.commit();
} catch (Exception e) {
  tm.rollback();
}
```