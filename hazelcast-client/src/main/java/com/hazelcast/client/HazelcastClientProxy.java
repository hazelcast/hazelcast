package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.serialization.TypeSerializer;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalTask;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * @mdogan 5/16/13
 */
public final class HazelcastClientProxy implements HazelcastInstance {

    volatile HazelcastClient client;

    HazelcastClientProxy(HazelcastClient client) {
        this.client = client;
    }

    public Config getConfig() {
        return getClient().getConfig();
    }

    public String getName() {
        return getClient().getName();
    }

    public <E> IQueue<E> getQueue(String name) {
        return getClient().getQueue(name);
    }

    public <E> ITopic<E> getTopic(String name) {
        return getClient().getTopic(name);
    }

    public <E> ISet<E> getSet(String name) {
        return getClient().getSet(name);
    }

    public <E> IList<E> getList(String name) {
        return getClient().getList(name);
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return getClient().getMap(name);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        return getClient().getMultiMap(name);
    }

    public ILock getLock(Object key) {
        return getClient().getLock(key);
    }

    public Cluster getCluster() {
        return getClient().getCluster();
    }

    public IExecutorService getExecutorService(String name) {
        return getClient().getExecutorService(name);
    }

    public <T> T executeTransaction(TransactionalTask<T> task) throws TransactionException {
        return getClient().executeTransaction(task);
    }

    public <T> T executeTransaction(TransactionOptions options, TransactionalTask<T> task) throws TransactionException {
        return getClient().executeTransaction(options, task);
    }

    public TransactionContext newTransactionContext() {
        return getClient().newTransactionContext();
    }

    public TransactionContext newTransactionContext(TransactionOptions options) {
        return getClient().newTransactionContext(options);
    }

    public IdGenerator getIdGenerator(String name) {
        return getClient().getIdGenerator(name);
    }

    public IAtomicLong getAtomicLong(String name) {
        return getClient().getAtomicLong(name);
    }

    public ICountDownLatch getCountDownLatch(String name) {
        return getClient().getCountDownLatch(name);
    }

    public ISemaphore getSemaphore(String name) {
        return getClient().getSemaphore(name);
    }

    public Collection<DistributedObject> getDistributedObjects() {
        return getClient().getDistributedObjects();
    }

    public String addDistributedObjectListener(DistributedObjectListener distributedObjectListener) {
        return getClient().addDistributedObjectListener(distributedObjectListener);
    }

    public boolean removeDistributedObjectListener(String registrationId) {
        return getClient().removeDistributedObjectListener(registrationId);
    }

    public PartitionService getPartitionService() {
        return getClient().getPartitionService();
    }

    public ClientService getClientService() {
        return getClient().getClientService();
    }

    public LoggingService getLoggingService() {
        return getClient().getLoggingService();
    }

    public LifecycleService getLifecycleService() {
        return getClient().getLifecycleService();
    }

    public <T extends DistributedObject> T getDistributedObject(Class<? extends RemoteService> serviceClass, Object id) {
        return getClient().getDistributedObject(serviceClass, id);
    }

    public <T extends DistributedObject> T getDistributedObject(String serviceName, Object id) {
        return getClient().getDistributedObject(serviceName, id);
    }

    public void registerSerializer(TypeSerializer serializer, Class type) {
        getClient().registerSerializer(serializer, type);
    }

    public void registerGlobalSerializer(TypeSerializer serializer) {
        getClient().registerGlobalSerializer(serializer);
    }

    public ConcurrentMap<String, Object> getUserContext() {
        return getClient().getUserContext();
    }

    private HazelcastClient getClient() {
        final HazelcastClient c = client;
        if (c == null) {
            throw new HazelcastInstanceNotActiveException();
        }
        return c;
    }
}
