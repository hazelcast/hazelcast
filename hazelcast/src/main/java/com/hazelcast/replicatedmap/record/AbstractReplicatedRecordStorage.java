package com.hazelcast.replicatedmap.record;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.CleanerRegistrator;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.NamedThreadFactory;
import com.hazelcast.util.nonblocking.NonBlockingHashMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractReplicatedRecordStorage<K, V> implements ReplicatedRecordStore {

    protected final ConcurrentMap<K, ReplicatedRecord<K, V>> storage = new NonBlockingHashMap<K, ReplicatedRecord<K, V>>();

    private final String name;
    private final Member localMember;
    private final int localMemberHash;
    private final NodeEngine nodeEngine;
    private final EventService eventService;
    private final String replicationTopicName;

    private final ExecutorService executorService;

    private final ReplicatedMapService replicatedMapService;
    private final ReplicatedMapConfig replicatedMapConfig;

    private final ScheduledFuture<?> cleanerFuture;
    private final Object[] mutexes;

    public AbstractReplicatedRecordStorage(String name, NodeEngine nodeEngine,
                                           CleanerRegistrator cleanerRegistrator,
                                           ReplicatedMapService replicatedMapService) {
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.localMember = nodeEngine.getLocalMember();
        this.localMemberHash = localMember.getUuid().hashCode();
        this.eventService = nodeEngine.getEventService();
        this.replicatedMapService = replicatedMapService;
        this.replicatedMapConfig = replicatedMapService.getReplicatedMapConfig(name);
        this.replicationTopicName = ReplicatedMapService.SERVICE_NAME + "-replication";
        this.executorService = getExecutorService(replicatedMapConfig);

        this.mutexes = new Object[replicatedMapConfig.getConcurrencyLevel()];
        for (int i = 0; i < mutexes.length; i++) {
            mutexes[i] = new Object();
        }

        this.cleanerFuture = cleanerRegistrator.registerCleaner(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object remove(Object key) {
        V old;
        synchronized (getMutex(key)) {
            final ReplicatedRecord current = storage.get(key);
            final Vector vector;
            if (current == null) {
                old = null;
            } else {
                vector = current.getVector();
                old = (V) current.getValue();
                current.setValue(null, 0);
                incrementClock(vector);
                publishReplicatedMessage(new ReplicationMessage(name, key, null, vector, localMember, localMemberHash));
            }
        }
        return old;
    }

    @Override
    public Object get(Object key) {
        ReplicatedRecord replicatedRecord = storage.get(key);
        return replicatedRecord == null ? null : replicatedRecord.getValue();
    }

    @Override
    public Object put(Object key, Object value) {
        V oldValue = null;
        synchronized (getMutex(key)) {
            final ReplicatedRecord old = storage.get(key);
            final Vector vector;
            if (old == null) {
                vector = new Vector();
                ReplicatedRecord<K, V> record = new ReplicatedRecord(key, value, vector, localMemberHash);
                storage.put((K) key, record);
            } else {
                oldValue = (V) old.getValue();
                vector = old.getVector();

                storage.get(key).setValue((V) value, localMemberHash);
            }
            incrementClock(vector);
            publishReplicatedMessage(new ReplicationMessage(name, key, value, vector, localMember, localMemberHash));
        }
        return oldValue;
    }

    @Override
    public boolean containsKey(Object key) {
        return storage.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (Map.Entry<K, ReplicatedRecord<K, V>> entry : storage.entrySet()) {
            V entryValue = entry.getValue().getValue();
            if(value == entryValue
                    || (entryValue != null && entryValue.equals(value))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ReplicatedRecord getReplicatedRecord(Object key) {
        return storage.get(key);
    }

    @Override
    public ReplicatedRecord putReplicatedRecord(Object key, ReplicatedRecord replicatedRecord) {
        return storage.put((K) key, replicatedRecord);
    }

    @Override
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public int size() {
        return storage.size();
    }

    @Override
    public void clear() {
        //TODO distribute clear
        storage.clear();
    }
    @Override
    public boolean equals(Object o) {
        return storage.equals(o);
    }

    @Override
    public int hashCode() {
        return storage.hashCode();
    }

    @Override
    public ReplicatedMapService getReplicatedMapService() {
        return replicatedMapService;
    }

    @Override
    public void destroy() {
        if (cleanerFuture.isCancelled()) {
            return;
        }
        cleanerFuture.cancel(true);
        executorService.shutdownNow();
        storage.clear();
        replicatedMapService.destroyDistributedObject(getName());
    }

    public Set<ReplicatedRecord> getRecords() {
        return new HashSet<ReplicatedRecord>(storage.values());
    }

    public void queueUpdateMessage(final ReplicationMessage update) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                publishReplicatedMessage(update);
            }
        });
    }

    protected void publishReplicatedMessage(IdentifiedDataSerializable message) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(ReplicatedMapService.SERVICE_NAME, replicationTopicName);
        eventService.publishEvent(ReplicatedMapService.SERVICE_NAME, registrations, message, name.hashCode());
    }

    protected void incrementClock(Vector vector) {
        final AtomicInteger clock = vector.clocks.get(localMember);
        if (clock != null) {
            clock.incrementAndGet();
        } else {
            vector.clocks.put(localMember, new AtomicInteger(1));
        }
    }

    protected Object getMutex(final Object key) {
        return mutexes[key.hashCode() != Integer.MIN_VALUE ? Math.abs(key.hashCode()) % mutexes.length : 0];
    }

    private void processUpdateMessage(ReplicationMessage update) {
        if (localMember.equals(update.getOrigin())) {
            return;
        }
        synchronized (getMutex(update.getKey())) {
            final ReplicatedRecord<K, V> localEntry = storage.get(update.getKey());
            if (localEntry == null) {
                if (!update.isRemove()) {
                    K key = (K) update.getKey();
                    V value = (V) update.getValue();
                    Vector vector = update.getVector();
                    int updateHash = update.getUpdateHash();
                    storage.put(key, new ReplicatedRecord<K, V>(key, value, vector, updateHash));
                }
            } else {
                final Vector currentVector = localEntry.getVector();
                final Vector updateVector = update.getVector();
                if (Vector.happenedBefore(updateVector, currentVector)) {
                    // ignore the update. This is an old update
                } else if (Vector.happenedBefore(currentVector, updateVector)) {
                    // A new update happened
                    applyTheUpdate(update, localEntry);
                } else {
                    // no preceding among the clocks. Lower hash wins..
                    if (localEntry.getLatestUpdateHash() >= update.getUpdateHash()) {
                        applyTheUpdate(update, localEntry);
                    } else {
                        applyVector(updateVector, currentVector);
                        publishReplicatedMessage(new ReplicationMessage(name, update.getKey(), localEntry.getValue(),
                                currentVector, localMember, localEntry.getLatestUpdateHash()));
                    }
                }
            }
        }
    }
    private void applyTheUpdate(ReplicationMessage<K, V> update, ReplicatedRecord<K, V> localEntry) {
        Vector localVector = localEntry.getVector();
        Vector remoteVector = update.getVector();
        localEntry.setValue(update.getValue(), update.getUpdateHash());
        applyVector(remoteVector, localVector);
    }

    private void applyVector(Vector update, Vector current) {
        for (Member m : update.clocks.keySet()) {
            final AtomicInteger currentClock = current.clocks.get(m);
            final AtomicInteger updateClock = update.clocks.get(m);
            if (smaller(currentClock, updateClock)) {
                current.clocks.put(m, new AtomicInteger(updateClock.get()));
            }
        }
    }

    private boolean smaller(AtomicInteger int1, AtomicInteger int2) {
        int i1 = int1 == null ? 0 : int1.get();
        int i2 = int2 == null ? 0 : int2.get();
        return i1 < i2;
    }

    private ExecutorService getExecutorService(ReplicatedMapConfig replicatedMapConfig) {
        ExecutorService es = replicatedMapConfig.getReplicatorExecutorService();
        if (es != null) {
            return new WrappedExecutorService(es);
        }
        return Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(ReplicatedMapService.SERVICE_NAME + "." + name + ".replicator"));
    }

}
