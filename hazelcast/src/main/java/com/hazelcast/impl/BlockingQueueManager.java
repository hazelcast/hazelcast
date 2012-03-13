/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.impl;

import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.impl.base.RuntimeInterruptedException;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.impl.monitor.LocalQueueStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.Packet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class BlockingQueueManager extends BaseManager {
    private final static long BILLION = 1000 * 1000 * 1000;

    BlockingQueueManager(Node node) {
        super(node);
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_ITERATE, new InitializationAwareOperationHandler() {
            @Override
            void doOperation(BQ queue, Request request) {
                queue.iterate(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_SIZE, new InitializationAwareOperationHandler() {
            void doOperation(BQ queue, Request request) {
                queue.size(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_GET_KEY_BY_INDEX, new InitializationAwareOperationHandler() {
            public void doOperation(BQ queue, Request request) {
                queue.doGetKeyByIndex(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_GET_INDEX_BY_KEY, new InitializationAwareOperationHandler() {
            public void doOperation(BQ queue, Request request) {
                queue.doGetIndexByKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_TAKE_KEY, new InitializationAwareOperationHandler() {
            public void doOperation(BQ queue, Request request) {
                queue.doTakeKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_CANCEL_TAKE_KEY, new InitializationAwareOperationHandler() {
            public void doOperation(BQ queue, Request request) {
                queue.cancelTakeKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_SET, new InitializationAwareOperationHandler() {
            public void doOperation(BQ queue, Request request) {
                queue.doSet(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_PEEK_KEY, new ResponsiveOperationHandler() {
            public void handle(Request request) {
                handlePeekKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_ADD_KEY, new ResponsiveOperationHandler() {
            public void handle(Request request) {
                handleAddKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_REMOVE_KEY, new InitializationAwareOperationHandler() {
            void doOperation(BQ queue, Request request) {
                queue.removeKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_GENERATE_KEY, new ResponsiveOperationHandler() {
            public void handle(Request request) {
                handleGenerateKey(request);
            }
        });
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_OFFER_KEY, new PacketProcessor() {
            public void process(Packet packet) {
                handleOfferKey(packet);
            }
        });
    }

    public void destroy(String name) {
        mapBQ.remove(name);
        node.listenerManager.removeAllRegisteredListeners(name);
    }

    public void syncForDead(MemberImpl deadMember) {
        for (BQ queue : mapBQ.values()) {
            queue.invalidateScheduledActionsFor(deadMember);
        }
    }

    abstract class InitializationAwareOperationHandler extends ResponsiveOperationHandler {
        abstract void doOperation(BQ queue, Request request);

        public void handle(Request request) {
            if (isMaster() && ready(request)) {
                BQ bq = getOrCreateBQ(request.name);
                doOperation(bq, request);
            } else {
                returnRedoResponse(request);
            }
        }
    }

    boolean addKeyAsync = false;

    private void sendKeyToMaster(final String queueName, final Data key, final int index) {
        enqueueAndReturn(new Processable() {
            public void process() {
                if (isMaster()) {
                    doAddKey(queueName, key, index);
                } else {
                    Packet packet = obtainPacket();
                    packet.name = queueName;
                    packet.setKey(key);
                    packet.operation = ClusterOperation.BLOCKING_OFFER_KEY;
                    packet.longValue = index;
                    boolean sent = send(packet, getMasterAddress());
                }
            }
        });
    }

    public int size(String name) {
        ThreadContext threadContext = ThreadContext.get();
        TransactionImpl txn = threadContext.getCallContext().getTransaction();
        int size = queueSize(name);
        if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
            size += txn.size(name);
        }
        return size;
    }

    public boolean remove(final String name, Object obj) {
        final Data dataValue = toData(obj);
        Set<Long> keys = getValueKeys(name, dataValue);
        if (keys != null) {
            for (Long key : keys) {
                Data keyData = toData(key);
                if (removeKey(name, keyData)) {
                    try {
                        getStorageMap(name).tryRemove(keyData, 0, TimeUnit.SECONDS);
                    } catch (TimeoutException ignored) {
                    }
                    final BQ bq = getBQ(name);
                    if (bq != null && bq.mapListeners.size() > 0) {
                        enqueueAndReturn(new Processable() {
                            public void process() {
                                fireMapEvent(bq.mapListeners, name, EntryEvent.TYPE_REMOVED, dataValue, thisAddress);
                            }
                        });
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public boolean add(String name, Object obj, int index) {
        try {
            return offer(name, obj, index, 0);
        } catch (InterruptedException ignored) {
            return false;
        }
    }

    public boolean offer(String name, Object obj, long timeout) throws InterruptedException {
        return offer(name, obj, Integer.MAX_VALUE, timeout);
    }

    public boolean offer(final String name, Object obj, int index, long timeout) throws InterruptedException {
        Long key = generateKey(name, timeout);
        ThreadContext threadContext = ThreadContext.get();
        TransactionImpl txn = threadContext.getCallContext().getTransaction();
        if (key != -1) {
            final Data dataItem = toData(obj);
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                txn.attachPutOp(name, key, dataItem, timeout, true);
            } else {
                storeQueueItem(name, key, dataItem, index);
                final BQ bq = getBQ(name);
                if (bq != null && bq.mapListeners.size() > 0) {
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            fireMapEvent(bq.mapListeners, name, EntryEvent.TYPE_ADDED, dataItem, thisAddress);
                        }
                    });
                }
            }
            return true;
        }
        return false;
    }

    public Object set(String name, Object newValue, int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        Data key = getKeyByIndex(name, index);
        if (key == null) {
            throw new IndexOutOfBoundsException();
        }
        IMap imap = getStorageMap(name);
        return imap.put(key, newValue);
    }

    public Object remove(String name, int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        Data key = null;
        try {
            key = takeKey(name, index, 0L);
        } catch (InterruptedException ignored) {
        }
        if (key == null) {
            throw new IndexOutOfBoundsException();
        }
        IMap imap = getStorageMap(name);
        try {
            return imap.tryRemove(key, 0, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return null;
        }
    }

    public void offerCommit(String name, Object key, Object obj) {
        storeQueueItem(name, key, obj, Integer.MAX_VALUE);
    }

    public void rollbackPoll(String name, Object key, Object obj) {
        final Data dataKey = toData(key);
        if (addKeyAsync) {
            sendKeyToMaster(name, dataKey, 0);
        } else {
            addKey(name, dataKey, 0);
        }
    }

    private void storeQueueItem(String name, Object key, Object obj, int index) {
        IMap imap = getStorageMap(name);
        final Data dataKey = toData(key);
        imap.put(dataKey, obj);
        if (addKeyAsync) {
            sendKeyToMaster(name, dataKey, index);
        } else {
            addKey(name, dataKey, index);
        }
    }

    public Object poll(final String name, long timeout) throws InterruptedException {
        if (timeout == -1) {
            timeout = Long.MAX_VALUE;
        }
        Object removedItem = null;
        long start = System.currentTimeMillis();
        while (removedItem == null && timeout >= 0) {
            Data key = takeKey(name, timeout);
            if (key == null) {
                return null;
            }
            IMap imap = getStorageMap(name);
            try {
                removedItem = imap.tryRemove(key, 0, TimeUnit.MILLISECONDS);
                if (removedItem != null) {
                    ThreadContext threadContext = ThreadContext.get();
                    TransactionImpl txn = threadContext.getCallContext().getTransaction();
                    final Data removedItemData = toData(removedItem);
                    if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                        txn.attachRemoveOp(name, key, removedItemData, true);
                    }
                    final BQ bq = getBQ(name);
                    if (bq != null && bq.mapListeners.size() > 0) {
                        enqueueAndReturn(new Processable() {
                            public void process() {
                                fireMapEvent(bq.mapListeners, name, EntryEvent.TYPE_REMOVED, removedItemData, thisAddress);
                            }
                        });
                    }
                }
            } catch (TimeoutException e) {
            }
            long now = System.currentTimeMillis();
            timeout -= (now - start);
            start = now;
        }
        return removedItem;
    }

    public Object peek(String name) {
        Data key = peekKey(name);
        if (key == null) {
            return null;
        }
        IMap imap = getStorageMap(name);
        return imap.get(key);
    }

    private Data takeKey(String name, long timeout) throws InterruptedException {
        return takeKey(name, -1, timeout);
    }

    private Data takeKey(String name, int index, long timeout) throws InterruptedException {
        try {
            MasterOp op = new MasterOp(ClusterOperation.BLOCKING_TAKE_KEY, name, timeout);
            op.request.longValue = index;
            op.request.txnId = ThreadContext.get().getThreadId();
            op.initOp();
            return (Data) op.getResultAsIs();
        } catch (Exception e) {
            if (e instanceof RuntimeInterruptedException) {
                MasterOp op = new MasterOp(ClusterOperation.BLOCKING_CANCEL_TAKE_KEY, name, timeout);
                op.request.longValue = index;
                op.request.txnId = ThreadContext.get().getThreadId();
                op.initOp();
                throw new InterruptedException();
            }
        }
        return null;
    }

    int getIndexOf(String name, Object obj, boolean first) {
        Set<Long> keys = getValueKeys(name, toData(obj));
        if (keys == null || keys.size() == 0) return -1;
        Long key = null;
        if (first) {
            key = keys.iterator().next();
        } else {
            key = (Long) keys.toArray()[keys.size() - 1];
        }
        return getIndexByKey(name, toData(key));
    }

    Set<Long> getValueKeys(String name, Data item) {
        while (true) {
            node.checkNodeState();
            try {
                return doGetValueKeys(name, item);
            } catch (Throwable e) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

    Set<Long> doGetValueKeys(String name, Data item) throws ExecutionException, InterruptedException {
        Set<Member> members = node.getClusterImpl().getMembers();
        List<Future<Keys>> lsFutures = new ArrayList<Future<Keys>>(members.size());
        for (Member member : members) {
            GetValueKeysCallable callable = new GetValueKeysCallable(name, item);
            DistributedTask<Keys> dt = new DistributedTask<Keys>(callable, member);
            lsFutures.add(dt);
            node.factory.getExecutorService().execute(dt);
        }
        Set<Long> foundKeys = new TreeSet<Long>();
        for (Future<Keys> future : lsFutures) {
            Keys keys = future.get();
            if (keys != null) {
                for (Data keyData : keys.getKeys()) {
                    foundKeys.add((Long) toObject(keyData));
                }
            }
        }
        return foundKeys;
    }

    public static class GetValueKeysCallable implements Callable<Keys>, DataSerializable, HazelcastInstanceAware {
        HazelcastInstance hazelcast;
        Data item;
        String name;

        public GetValueKeysCallable() {
        }

        public GetValueKeysCallable(String name, Data item) {
            this.name = name;
            this.item = item;
        }

        public Keys call() throws Exception {
            IMap imap = hazelcast.getMap(name);
            Set localKeys = imap.localKeySet();
            if (localKeys != null) {
                Object itemObject = toObject(item);
                Keys keys = new Keys();
                for (Object key : localKeys) {
                    Object v = imap.get(key);
                    if (v != null && v.equals(itemObject)) {
                        keys.add(toData(key));
                    }
                }
                return keys;
            }
            return null;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(name);
            item.writeData(out);
        }

        public void readData(DataInput in) throws IOException {
            name = in.readUTF();
            item = new Data();
            item.readData(in);
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcast = hazelcastInstance;
        }
    }

    Object getItemByIndex(String name, int index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        Data key = getKeyByIndex(name, index);
        if (key == null) {
            throw new IndexOutOfBoundsException();
        }
        IMap imap = getStorageMap(name);
        return imap.get(key);
    }

    private Data getKeyByIndex(String name, int index) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_GET_KEY_BY_INDEX, name, 0);
        op.request.longValue = index;
        op.initOp();
        return (Data) op.getResultAsIs();
    }

    private Integer getIndexByKey(String name, Data keyData) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_GET_INDEX_BY_KEY, name, 0);
        op.request.key = keyData;
        op.initOp();
        return (Integer) op.getResultAsObject();
    }

    private Data peekKey(String name) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_PEEK_KEY, name, 0);
        op.initOp();
        return (Data) op.getResultAsIs();
    }

    public IMap getStorageMap(String queueName) {
        return node.factory.getMap(queueName);
    }

    CMap getStorageCMap(String queueName) {
        return node.concurrentMapManager.getMap(Prefix.MAP + queueName);
    }

    CMap getOrCreateStorageCMap(String queueName) {
        return node.concurrentMapManager.getOrCreateMap(Prefix.MAP + queueName);
    }

    public Iterator iterate(final String name) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_ITERATE, name, 0);
        op.initOp();
        Keys keys = (Keys) op.getResultAsObject();
        final Collection<Data> dataKeys = keys.getKeys();
        final Collection allKeys = new ArrayList(dataKeys);
        TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
        Map txnOfferItems = null;
        if (txn != null) {
            txnOfferItems = txn.newKeys(name);
            if (txnOfferItems != null) {
                allKeys.addAll(txnOfferItems.keySet());
            }
        }
        final Map txnMap = txnOfferItems;
        final Iterator it = allKeys.iterator();
        final IMap imap = getStorageMap(name);
        return new Iterator() {
            Object key = null;
            Object next = null;
            boolean hasNext = false;
            boolean set = false;

            public boolean hasNext() {
                if (!set) {
                    set();
                }
                boolean result = hasNext;
                hasNext = false;
                set = false;
                return result;
            }

            public Object next() {
                if (!set) {
                    set();
                }
                Object result = next;
                set = false;
                next = null;
                return result;
            }

            public void remove() {
                if (key != null) {
                    try {
                        Data dataKey = toData(key);
                        imap.tryRemove(dataKey, 0, TimeUnit.MILLISECONDS);
                        removeKey(name, dataKey);
                    } catch (TimeoutException ignored) {
                    }
                }
            }

            void set() {
                try {
                    while (next == null) {
                        hasNext = it.hasNext();
                        if (hasNext) {
                            key = it.next();
                            if (txnMap != null) {
                                next = txnMap.get(key);
                            }
                            if (next == null) {
                                next = imap.get(key);
                            }
                        } else {
                            return;
                        }
                    }
                } finally {
                    set = true;
                }
            }
        };
    }

    public boolean addKey(String name, Data key, int index) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_ADD_KEY, name, 0);
        op.request.key = key;
        op.request.setBooleanRequest();
        op.request.longValue = index;
        op.initOp();
        return op.getResultAsBoolean();
    }

    public Data set(String name, Data key, int index) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_SET, name, 0);
        op.request.key = key;
        op.request.setBooleanRequest();
        op.request.longValue = index;
        op.initOp();
        return (Data) op.getResultAsIs();
    }

    public boolean removeKey(String name, Data key) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_REMOVE_KEY, name, 0);
        op.request.key = key;
        op.request.setBooleanRequest();
        op.initOp();
        return op.getResultAsBoolean();
    }

    public long generateKey(String name, long timeout) throws InterruptedException {
        try {
            MasterOp op = new MasterOp(ClusterOperation.BLOCKING_GENERATE_KEY, name, timeout);
            op.request.setLongRequest();
            op.request.txnId = ThreadContext.get().getThreadId();
            op.initOp();
            return (Long) op.getResultAsObject();
        } catch (Exception e) {
            if (e instanceof RuntimeInterruptedException) {
                throw new InterruptedException();
            }
        }
        return -1;
    }

    public int queueSize(String name) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_SIZE, name, 0);
        op.request.setLongRequest();
        op.initOp();
        return ((Long) op.getResultAsObject()).intValue();
    }

    long getKey(String queueName) {
        return node.factory.getIdGenerator(queueName).newId();
    }

    class MasterOp extends TargetAwareOp {
        private final ClusterOperation op;
        private final String name;
        private final long timeout;

        MasterOp(ClusterOperation op, String name, long timeout) {
            this.op = op;
            this.name = name;
            this.timeout = timeout;
        }

        @Override
        public void setTarget() {
            target = getMasterAddress();
        }

        void initOp() {
            request.operation = op;
            request.name = name;
            request.timeout = timeout;
            doOp();
        }
    }

    class Lease {
        final long timeout;
        final Address address;

        Lease(Address address) {
            this.address = address;
            timeout = System.currentTimeMillis() + 10000;
        }
    }

    final Map<String, BQ> mapBQ = new ConcurrentHashMap<String, BQ>();

    BQ getOrCreateBQ(String name) {
        BQ bq = mapBQ.get(name);
        if (bq == null) {
            bq = new BQ(name);
            mapBQ.put(name, bq);
        }
        return bq;
    }

    BQ getBQ(String name) {
        return mapBQ.get(name);
    }

    final void handlePeekKey(Request req) {
        if (isMaster() && ready(req)) {
            BQ bq = getOrCreateBQ(req.name);
            bq.doPeekKey(req);
        } else {
            returnRedoResponse(req);
        }
    }

    final void handleOfferKey(Packet packet) {
        if (isMaster()) {
            doAddKey(packet.name, packet.getKeyData(), (int) packet.longValue);
        }
        releasePacket(packet);
    }

    final void doAddKey(String name, Data key, int index) {
        BQ bq = getOrCreateBQ(name);
        bq.doAddKey(key, index);
    }

    final void handleAddKey(Request req) {
        if (isMaster() && ready(req)) {
            BQ bq = getOrCreateBQ(req.name);
            bq.doAddKey(req.key, (int) req.longValue);
            req.key = null;
            req.response = Boolean.TRUE;
            returnResponse(req);
        } else {
            returnRedoResponse(req);
        }
    }

    final void handleGenerateKey(Request req) {
        if (isMaster() && ready(req)) {
            BQ bq = getOrCreateBQ(req.name);
            bq.doGenerateKey(req);
        } else {
            returnRedoResponse(req);
        }
    }

    enum MasterState {
        NOT_INITIALIZED,
        INITIALIZING,
        READY
    }

    boolean ready(Request request) {
        BQ q = getOrCreateBQ(request.name);
        if (q.state == MasterState.READY) {
            return true;
        } else if (q.state == MasterState.NOT_INITIALIZED) {
            q.state = MasterState.INITIALIZING;
            initialize(request.name);
        }
        return false;
    }

    void initialize(final String queueName) {
        final BQ q = getOrCreateBQ(queueName);
        final CMap cmapStorage = getOrCreateStorageCMap(queueName);
        executeLocally(new Runnable() {
            public void run() {
                TreeSet itemKeys = null;
                if (cmapStorage.loader != null) {
                    Set keys = cmapStorage.loader.loadAllKeys();
                    if (keys != null && keys.size() > 0) {
                        itemKeys = new TreeSet<Long>(keys);
                    }
                }
                Set keys = getStorageMap(queueName).keySet();
                if (keys != null && keys.size() > 0) {
                    if (itemKeys == null) {
                        itemKeys = new TreeSet<Long>(keys);
                    } else {
                        itemKeys.addAll(keys);
                    }
                }
                if (itemKeys != null) {
                    final Set<Long> queueKeys = itemKeys;
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            for (Long key : queueKeys) {
                                Data keyData = toData(key);
                                if (q.keys.add(keyData)) {
                                    q.queue.add(new QData(keyData));
                                    q.nextKey = Math.max(q.nextKey, key);
                                }
                            }
                            q.nextKey += BILLION;
                            q.state = MasterState.READY;
                        }
                    });
                } else {
                    q.state = MasterState.READY;
                }
            }
        });
    }

    public void addItemListener(final String name, final ItemListener listener, final boolean includeValue) {
        node.listenerManager.addListener(name, listener, null, includeValue, Instance.InstanceType.QUEUE);
    }

    public void removeItemListener(final String name, final ItemListener listener) {
        List<ListenerManager.ListenerItem> lsListenerItems = node.listenerManager.getOrCreateListenerList(name);
        for (ListenerManager.ListenerItem listenerItem : lsListenerItems) {
            if (listenerItem.listener == listener) {
                lsListenerItems.remove(listenerItem);
                return;
            }
        }
    }

    void registerListener(boolean add, String name, Data key, Address address, boolean includeValue) {
        BQ queue = getOrCreateBQ(name);
        if (add) {
            queue.mapListeners.put(address, includeValue);
        } else {
            queue.mapListeners.remove(address);
        }
    }

    class BQ {
        final Map<Address, Boolean> mapListeners = new ConcurrentHashMap<Address, Boolean>(1);
        final LinkedList<ScheduledAction> offerWaitList = new LinkedList<ScheduledAction>();
        final LinkedList<PollAction> pollWaitList = new LinkedList<PollAction>();
        final LinkedList<Lease> leases = new LinkedList<Lease>();
        final LinkedList<QData> queue = new LinkedList<QData>();
        final Set<Data> keys = new HashSet<Data>(1000);
        final int maxSizePerJVM;
        final long ttl;
        final String name;
        final QueueConfig queueConfig;
        long nextKey = 0;
        volatile MasterState state = MasterState.NOT_INITIALIZED;

        BQ(String name) {
            this.name = name;
            String shortName = name.substring(Prefix.QUEUE.length());
            queueConfig = node.getConfig().findMatchingQueueConfig(shortName);
            MapConfig backingMapConfig = node.getConfig().findMatchingMapConfig(queueConfig.getBackingMapRef());
            int backingMapTTL = backingMapConfig.getTimeToLiveSeconds();
            this.maxSizePerJVM = (queueConfig.getMaxSizePerJVM() == 0) ? Integer.MAX_VALUE : queueConfig.getMaxSizePerJVM();
            this.ttl = (backingMapTTL == 0) ? Integer.MAX_VALUE : TimeUnit.SECONDS.toMillis(backingMapTTL);
            initializeListeners();
        }

        private void initializeListeners() {
            for (ItemListenerConfig lc : queueConfig.getItemListenerConfigs()) {
                try {
                    node.listenerManager.createAndAddListenerItem(name, lc, Instance.InstanceType.QUEUE);
                    for (MemberImpl member : node.clusterManager.getMembers()) {
                        mapListeners.put(member.getAddress(), lc.isIncludeValue());
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }

        int maxSize() {
            return (maxSizePerJVM == Integer.MAX_VALUE) ? Integer.MAX_VALUE : maxSizePerJVM * lsMembers.size();
        }

        void doGenerateKey(Request req) {
            if (size() >= maxSize()) {
                if (req.hasEnoughTimeToSchedule()) {
                    addOfferAction(new OfferAction(req));
                } else {
                    req.response = -1L;
                    returnResponse(req);
                }
            } else {
                generateKeyAndLease(req);
                returnResponse(req);
            }
        }

        void size(Request req) {
            req.response = Long.valueOf(queue.size());
            returnResponse(req);
        }

        void generateKeyAndLease(Request req) {
            leases.add(new Lease(req.caller));
            req.response = nextKey++;
        }

        void doSet(Request req) {
            int index = (int) req.longValue;
            Data key = req.key;
            Data oldKey = null;
            boolean added = false;
            if (queue.size() >= index) {
                queue.add(new QData(key));
                added = true;
            } else {
                QData old = queue.set(index, new QData(key));
                if (isValid(old, System.currentTimeMillis())) {
                    oldKey = old.data;
                } else {
                    added = true;
                }
            }
            if (added) {
                takeOne();
            }
            req.response = oldKey;
            returnResponse(req);
        }

        void doAddKey(Data key, int index) {
            if (keys.add(key)) {
                if (leases.size() > 0) {
                    leases.removeFirst();
                }
                if (index == Integer.MAX_VALUE || index >= queue.size()) {
                    queue.add(new QData(key));
                } else if (index == 0) {
                    queue.addFirst(new QData(key));
                } else {
                    queue.add(index, new QData(key));
                }
                takeOne();
            }
        }

        public void removeKey(Request request) {
            if (keys.remove(request.key)) {
                Iterator<QData> it = queue.iterator();
                while (it.hasNext()) {
                    QData qData = it.next();
                    if (qData.data.equals(request.key)) {
                        it.remove();
                        request.response = Boolean.TRUE;
                        break;
                    }
                }
            }
            if (request.response == null) {
                request.response = Boolean.FALSE;
            }
            returnResponse(request);
        }

        void takeOne() {
            while (pollWaitList.size() > 0) {
                ScheduledAction scheduledActionPoll = pollWaitList.removeFirst();
                if (!scheduledActionPoll.expired() && scheduledActionPoll.isValid()) {
                    scheduledActionPoll.consume();
                    node.clusterManager.deregisterScheduledAction(scheduledActionPoll);
                    return;
                }
            }
        }

        void offerOne() {
            while (offerWaitList.size() > 0) {
                ScheduledAction scheduledActionOffer = offerWaitList.removeFirst();
                if (!scheduledActionOffer.expired() && scheduledActionOffer.isValid()) {
                    scheduledActionOffer.consume();
                    node.clusterManager.deregisterScheduledAction(scheduledActionOffer);
                    return;
                }
            }
        }

        QData pollValidItem() {
            QData qdata = queue.poll();
            if (qdata == null) {
                return null;
            }
            long now = System.currentTimeMillis();
            if (isValid(qdata, now)) {
                return qdata;
            }
            while (qdata != null) {
                qdata = queue.poll();
                if (isValid(qdata, now)) {
                    return qdata;
                }
            }
            return qdata;
        }

        QData removeItemByIndex(int index) {
            if (index >= queue.size()) {
                return null;
            }
            return queue.remove(index);
        }

        boolean isValid(QData qdata, long now) {
            return qdata != null && (now - qdata.createDate) < ttl;
        }

        void cancelTakeKey(Request req) {
            cancelPollAction(req);
        }

        void doTakeKey(Request req) {
            QData qdata;
            if (req.longValue > 0) {
                qdata = removeItemByIndex((int) req.longValue);
            } else {
                qdata = pollValidItem();
            }
            if (qdata != null) {
                keys.remove(qdata.data);
                req.response = qdata.data;
                returnResponse(req);
                offerOne();
            } else {
                if (req.hasEnoughTimeToSchedule()) {
                    addPollAction(new PollAction(req));
                } else {
                    req.response = null;
                    returnResponse(req);
                }
            }
        }

        void doGetIndexByKey(Request req) {
            Data keyData = req.key;
            int i = -1;
            for (QData qdata : queue) {
                if (qdata.data.equals(keyData)) {
                    i++;
                    break;
                } else {
                    i++;
                }
            }
            req.response = toData(i);
            returnResponse(req);
        }

        void doGetKeyByIndex(Request req) {
            int index = (int) req.longValue;
            long now = System.currentTimeMillis();
            int i = 0;
            QData key = null;
            for (QData qdata : queue) {
                if (isValid(qdata, now)) {
                    if (index == i) {
                        key = qdata;
                        break;
                    }
                    i++;
                }
            }
            req.response = (key == null) ? null : key.data;
            returnResponse(req);
        }

        void doPeekKey(Request req) {
            QData qdata = queue.peek();
            req.response = (qdata == null) ? null : qdata.data;
            returnResponse(req);
        }

        void addPollAction(PollAction pollAction) {
            pollWaitList.add(pollAction);
            node.clusterManager.registerScheduledAction(pollAction);
        }

        void cancelPollAction(Request req) {
            PollAction toCancel = null;
            for (PollAction pollAction : pollWaitList) {
                Request pReq = pollAction.getRequest();
                if (pReq.caller.equals(req.caller) && pReq.longValue == req.longValue && pReq.txnId == req.txnId) {
                    toCancel = pollAction;
                }
            }
            if (toCancel != null) {
                pollWaitList.remove(toCancel);
                node.clusterManager.deregisterScheduledAction(toCancel);
            }
        }

        void addOfferAction(OfferAction offerAction) {
            offerWaitList.add(offerAction);
            node.clusterManager.registerScheduledAction(offerAction);
        }

        public int size() {
            return queue.size() + leases.size();
        }

        public void iterate(Request request) {
            Keys keys = new Keys();
            for (QData qData : queue) {
                keys.add(qData.data);
            }
            request.response = keys;
            returnResponse(request);
        }

        public int getMaxSizePerJVM() {
            return maxSizePerJVM;
        }

        public LocalQueueStatsImpl getQueueStats() {
            long now = System.currentTimeMillis();
            CMap cmap = getStorageCMap(name);
            IMap storageMap = getStorageMap(name);
            Set<Object> localKeys = storageMap.localKeySet();
            int total = cmap != null ? cmap.mapRecords.size() : 0;
            int ownedCount = localKeys.size();
            int backupCount = Math.abs(total - ownedCount);
            long minAge = Long.MAX_VALUE;
            long maxAge = Long.MIN_VALUE;
            long totalAge = 0;
            for (Object localKey : localKeys) {
                MapEntry entry = storageMap.getMapEntry(localKey);
                if (entry != null) {
                    long age = (now - entry.getCreationTime());
                    minAge = Math.min(minAge, age);
                    maxAge = Math.max(maxAge, age);
                    totalAge += age;
                }
            }
            long aveAge = (ownedCount == 0) ? 0 : (totalAge / ownedCount);
            return new LocalQueueStatsImpl(ownedCount, backupCount, minAge, maxAge, aveAge);
        }

        public void invalidateScheduledActionsFor(MemberImpl deadMember) {
            for (PollAction pollAction : pollWaitList) {
                if (deadMember.address.equals(pollAction.getRequest().caller)) {
                    pollAction.setValid(false);
                    node.clusterManager.deregisterScheduledAction(pollAction);
                }
            }
            for (ScheduledAction offerAction : offerWaitList) {
                if (deadMember.address.equals(offerAction.getRequest().caller)) {
                    offerAction.setValid(false);
                    node.clusterManager.deregisterScheduledAction(offerAction);
                }
            }
        }

        public class OfferAction extends ScheduledAction {

            public OfferAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                generateKeyAndLease(request);
                returnResponse(request);
                setValid(false);
                return true;
            }

            @Override
            public void onExpire() {
                request.response = -1L;
                offerWaitList.remove(this);
                returnResponse(request);
                setValid(false);
            }
        }

        public class PollAction extends ScheduledAction {

            public PollAction(Request request) {
                super(request);
            }

            @Override
            public boolean consume() {
                doTakeKey(request);
                setValid(false);
                return true;
            }

            @Override
            public void onExpire() {
                request.response = null;
                pollWaitList.remove(this);
                returnResponse(request);
                setValid(false);
            }
        }
    }

    class QData {
        final Data data;
        final long createDate;

        QData(Data data) {
            this.data = data;
            this.createDate = System.currentTimeMillis();
        }
    }
}
