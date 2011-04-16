/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.base.PacketProcessor;
import com.hazelcast.impl.base.RuntimeInterruptedException;
import com.hazelcast.impl.base.ScheduledAction;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toData;

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
        node.clusterService.registerPacketProcessor(ClusterOperation.BLOCKING_TAKE_KEY, new InitializationAwareOperationHandler() {
            public void doOperation(BQ queue, Request request) {
                queue.doTakeKey(request);
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

    private void sendKeyToMaster(final String queueName, final Data key, final boolean last) {
        enqueueAndReturn(new Processable() {
            public void process() {
                if (isMaster()) {
                    doAddKey(queueName, key, last);
                } else {
                    Packet packet = obtainPacket();
                    packet.name = queueName;
                    packet.setKey(key);
                    packet.operation = ClusterOperation.BLOCKING_OFFER_KEY;
                    packet.longValue = (last) ? 0 : 1;
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

    public boolean offer(String name, Object obj, long timeout) throws InterruptedException {
        Long key = generateKey(name, timeout);
        ThreadContext threadContext = ThreadContext.get();
        TransactionImpl txn = threadContext.getCallContext().getTransaction();
        if (key != -1) {
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                txn.attachPutOp(name, key, obj, timeout, true);
            } else {
                storeQueueItem(name, key, obj, true);
            }
            return true;
        }
        return false;
    }

    public void offerCommit(String name, Object key, Object obj) {
        storeQueueItem(name, key, obj, true);
    }

    public void rollbackPoll(String name, Object key, Object obj) {
        storeQueueItem(name, key, obj, false);
    }

    private void storeQueueItem(String name, Object key, Object obj, boolean last) {
        IMap imap = getStorageMap(name);
        final Data dataKey = toData(key);
        imap.put(dataKey, obj);
        if (addKeyAsync) {
            sendKeyToMaster(name, dataKey, last);
        } else {
            addKey(name, dataKey, last);
        }
    }

    public Object poll(String name, long timeout) throws InterruptedException {
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
                    if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                        txn.attachRemoveOp(name, key, removedItem, true);
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
        try {
            MasterOp op = new MasterOp(ClusterOperation.BLOCKING_TAKE_KEY, name, timeout);
            op.initOp();
            return (Data) op.getResultAsIs();
        } catch (Exception e) {
            if (e instanceof RuntimeInterruptedException) {
                throw new InterruptedException();
            }
        }
        return null;
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

    public boolean addKey(String name, Data key, boolean last) {
        MasterOp op = new MasterOp(ClusterOperation.BLOCKING_ADD_KEY, name, 0);
        op.request.key = key;
        op.request.setBooleanRequest();
        op.request.longValue = (last) ? 0 : 1;
        op.initOp();
        return op.getResultAsBoolean();
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

    final Map<String, BQ> mapBQ = new HashMap<String, BQ>();

    BQ getOrCreateBQ(String name) {
        BQ bq = mapBQ.get(name);
        if (bq == null) {
            bq = new BQ(name);
            mapBQ.put(name, bq);
        }
        return bq;
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
            doAddKey(packet.name, packet.getKeyData(), (packet.longValue == 0) ? true : false);
        }
        releasePacket(packet);
    }

    final void doAddKey(String name, Data key, boolean last) {
        BQ bq = getOrCreateBQ(name);
        bq.doAddKey(key, last);
    }

    final void handleAddKey(Request req) {
        if (isMaster() && ready(req)) {
            BQ bq = getOrCreateBQ(req.name);
            bq.doAddKey(req.key, (req.longValue == 0) ? true : false);
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
                TreeSet<Long> itemKeys = null;
                if (cmapStorage.loader != null) {
                    Set<Long> keys = cmapStorage.loader.loadAllKeys();
                    if (keys != null && keys.size() > 0) {
                        itemKeys = new TreeSet<Long>(keys);
                    }
                }
                Set<Long> keys = getStorageMap(queueName).keySet();
                if (keys.size() > 0) {
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
        IMap map = getStorageMap(name);
        map.addEntryListener(new QueueItemListener(listener, includeValue), includeValue);
    }

    class QueueItemListener implements EntryListener {
        final ItemListener itemListener;
        final boolean includeValue;

        QueueItemListener(ItemListener itemListener, boolean includeValue) {
            this.includeValue = includeValue;
            this.itemListener = itemListener;
        }

        public void entryAdded(EntryEvent entryEvent) {
            Object item = (includeValue) ? entryEvent.getValue() : null;
            itemListener.itemAdded(item);
        }

        public void entryRemoved(EntryEvent entryEvent) {
            Object item = (includeValue) ? entryEvent.getValue() : null;
            itemListener.itemRemoved(item);
        }

        public void entryUpdated(EntryEvent entryEvent) {
        }

        public void entryEvicted(EntryEvent entryEvent) {
        }
    }

    public void removeItemListener(final String name, final ItemListener listener) {
        List<ListenerManager.ListenerItem> lsListenerItems = node.listenerManager.getListeners();
        for (ListenerManager.ListenerItem listenerItem : lsListenerItems) {
            if (listenerItem.listener instanceof QueueItemListener) {
                QueueItemListener queueListener = (QueueItemListener) listenerItem.listener;
                if (queueListener.itemListener == listener) {
                    node.listenerManager.getListeners().remove(listenerItem);
                    return;
                }
            }
        }
    }

    class BQ {
        final List<ScheduledAction> offerWaitList = new ArrayList<ScheduledAction>(1000);
        final List<ScheduledAction> pollWaitList = new ArrayList<ScheduledAction>(1000);
        final List<Lease> leases = new ArrayList<Lease>(1000);
        final Set<Data> keys = new HashSet<Data>(1000);
        final LinkedList<QData> queue = new LinkedList<QData>();
        final int maxSizePerJVM;
        final long ttl;
        final String name;
        long nextKey = 0;
        volatile MasterState state = MasterState.NOT_INITIALIZED;

        BQ(String name) {
            this.name = name;
            String shortName = name.substring(Prefix.QUEUE.length());
            QueueConfig qConfig = node.getConfig().findMatchingQueueConfig(shortName);
            MapConfig backingMapConfig = node.getConfig().findMatchingMapConfig(qConfig.getBackingMapRef());
            int backingMapTTL = backingMapConfig.getTimeToLiveSeconds();
            this.maxSizePerJVM = (qConfig.getMaxSizePerJVM() == 0) ? Integer.MAX_VALUE : qConfig.getMaxSizePerJVM();
            this.ttl = (backingMapTTL == 0) ? Integer.MAX_VALUE : TimeUnit.SECONDS.toMillis(backingMapTTL);
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

        void offerKey(Request req) {
            if (size() > maxSize()) {
                req.response = OBJECT_REDO;
            } else {
                leases.add(new Lease(req.caller));
                req.response = Boolean.TRUE;
            }
            returnResponse(req);
        }

        void doAddKey(Data key, boolean last) {
            if (keys.add(key)) {
                if (leases.size() > 0) {
                    leases.remove(0);
                }
                if (last) {
                    queue.offer(new QData(key));
                } else {
                    queue.addFirst(new QData(key));
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
                ScheduledAction scheduledActionPoll = pollWaitList.remove(0);
                if (!scheduledActionPoll.expired() && scheduledActionPoll.isValid()) {
                    scheduledActionPoll.consume();
                    return;
                }
            }
        }

        void offerOne() {
            while (offerWaitList.size() > 0) {
                ScheduledAction scheduledActionOffer = offerWaitList.remove(0);
                if (!scheduledActionOffer.expired() && scheduledActionOffer.isValid()) {
                    scheduledActionOffer.consume();
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

        boolean isValid(QData qdata, long now) {
            return qdata != null && (now - qdata.createDate) < ttl;
        }

        void doTakeKey(Request req) {
            QData qdata = pollValidItem();
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

        void doPeekKey(Request req) {
            QData qdata = queue.peek();
            req.response = (qdata == null) ? null : qdata.data;
            returnResponse(req);
        }

        void addPollAction(PollAction pollAction) {
            pollWaitList.add(pollAction);
            node.clusterManager.registerScheduledAction(pollAction);
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
