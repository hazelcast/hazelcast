

## XA Transactions

XA describes the interface between the global transaction manager and the local resource manager. The goal of XA is to allow multiple resources (such as databases, application servers, message queues, transactional caches, etc.) to be accessed within the same transaction, thereby preserving the ACID properties across applications. XA uses a two-phase commit to ensure that all resources either commit or rollback any particular transaction consistently (all do the same).

By implementing `XAResource` interface, Hazelcast provides XA transactions. `XAResource` instance can be obtained via `TransactionContext`.
Below is a sample code which uses Atomikos for transaction management.
  
      UserTransactionManager tm = new UserTransactionManager();
      tm.setTransactionTimeout(60);
      tm.begin();

      HazelcastInstance instance = Hazelcast.newHazelcastInstance();
      TransactionContext context = instance.newTransactionContext();
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
