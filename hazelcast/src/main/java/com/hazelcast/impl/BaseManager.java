/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.RemotelyProcessable;
import com.hazelcast.core.EntryEvent;
import static com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.core.Member;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.ResponseTypes.*;
import com.hazelcast.nio.*;
import static com.hazelcast.nio.BufferUtil.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseManager {

    protected final static boolean zeroBackup = false;

    public static final int EVENT_QUEUE_COUNT = 100;

    protected final Logger logger = Logger.getLogger(BaseManager.class.getName());

    protected final LinkedList<MemberImpl> lsMembers;

    protected final Map<Address, MemberImpl> mapMembers;

    protected final Map<Long, Call> mapCalls;

    protected final EventQueue[] eventQueues;

    protected final Map<Long, StreamResponseHandler> mapStreams;

    private static long scheduledActionIdIndex = 0;

    private static long callIdGen = 0;

    protected final Address thisAddress;

    protected final MemberImpl thisMember;

    protected final Node node;

    protected BaseManager(Node node) {
        this.node = node;
        lsMembers = node.baseVariables.lsMembers;
        mapMembers = node.baseVariables.mapMembers;
        mapCalls = node.baseVariables.mapCalls;
        eventQueues = node.baseVariables.eventQueues;
        mapStreams = node.baseVariables.mapStreams;
        thisAddress = node.baseVariables.thisAddress;
        thisMember = node.baseVariables.thisMember;
    }

    public LinkedList<MemberImpl> getMembers() {
        return lsMembers;
    }

    public Address getThisAddress() {
        return thisAddress;
    }

    public abstract class ScheduledAction {
        protected long timeToExpire;

        protected long timeout;

        protected boolean valid = true;

        protected Request request = null;

        protected final long id;

        public ScheduledAction(final Request request) {
            this.request = request;
            setTimeout(request.timeout);
            id = scheduledActionIdIndex++;
        }

        public abstract boolean consume();

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final ScheduledAction other = (ScheduledAction) obj;
            return getOuterType().equals(other.getOuterType()) && id == other.id;
        }

        public boolean expired() {
            return !valid || timeout != -1 && System.currentTimeMillis() >= getExpireTime();
        }

        public long getExpireTime() {
            return timeToExpire;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + (int) (id ^ (id >>> 32));
            return result;
        }

        public boolean isValid() {
            return valid;
        }

        public boolean neverExpires() {
            return (timeout == -1);
        }

        public void onExpire() {

        }

        public void setTimeout(long timeout) {
            if (timeout > -1) {
                this.timeout = timeout;
                timeToExpire = System.currentTimeMillis() + timeout;
            } else {
                this.timeout = -1;
            }
        }

        public void setValid(final boolean valid) {
            this.valid = valid;
        }

        @Override
        public String toString() {
            return "ScheduledAction {neverExpires=" + neverExpires() + ", timeout= " + timeout
                    + "}";
        }

        private BaseManager getOuterType() {
            return BaseManager.this;
        }
    }

    public static class Pairs implements DataSerializable {
        List<KeyValue> lsKeyValues = null;

        public Pairs() {
        }

        public void addKeyValue(KeyValue keyValue) {
            if (lsKeyValues == null) {
                lsKeyValues = new ArrayList<KeyValue>();
            }
            lsKeyValues.add(keyValue);
        }

        public void writeData(DataOutput out) throws IOException {
            int size = (lsKeyValues == null) ? 0 : lsKeyValues.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                lsKeyValues.get(i).writeData(out);
            }
        }

        public void readData(DataInput in) throws IOException {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                if (lsKeyValues == null) {
                    lsKeyValues = new ArrayList<KeyValue>();
                }
                KeyValue kv = new KeyValue();
                kv.readData(in);
                lsKeyValues.add(kv);
            }
        }

        public long size() {
            return (lsKeyValues == null) ? 0 : lsKeyValues.size();
        }

    }

    public static Map.Entry createSimpleEntry(final FactoryImpl factory, final String name, final Object key, final Object value) {
        return new Map.Entry() {
            public Object getKey() {
                return key;
            }

            public Object getValue() {
                return value;
            }

            public Object setValue(Object newValue) {
                return ((FactoryImpl.MProxy) factory.getProxyByName(name)).put(key, newValue);
            }

            @Override
            public String toString() {
                return "Map.Entry key=" + getKey() + ", value=" + getValue();
            }
        };
    }

    protected void rethrowException(ClusterOperation operation, AddressAwareException exception) {
        String msg = operation + " failed at " + thisAddress
                + " because of an exception thrown at " + exception.getAddress();
        throw new RuntimeException(msg, exception.getException());
    }

    public static class AddressAwareException implements Serializable {
        private Exception exception;
        private Address address; //where the exception happened

        public AddressAwareException(Exception exception, Address address) {
            this.exception = exception;
            this.address = address;
        }

        public AddressAwareException() {
        }

        public Exception getException() {
            return exception;
        }

        public Address getAddress() {
            return address;
        }
    }

    public static class KeyValue implements Map.Entry, DataSerializable {
        Data key = null;
        Data value = null;
        Object objKey = null;
        Object objValue = null;
        String name = null;
        String factoryName = null;

        public KeyValue() {
        }

        public KeyValue(Data key, Data value) {
            this.key = key;
            this.value = value;
        }

        public void writeData(DataOutput out) throws IOException {
            key.writeData(out);
            boolean gotValue = (value != null && value.size() > 0);
            out.writeBoolean(gotValue);
            if (gotValue) {
                value.writeData(out);
            }

        }

        public void readData(DataInput in) throws IOException {
            key = new Data();
            key.readData(in);
            boolean gotValue = in.readBoolean();
            if (gotValue) {
                value = new Data();
                value.readData(in);
            }
        }

        public Object getKey() {
            if (objKey == null) {
                objKey = toObject(key);
                key = null; // consumed on toObject(key)
            }
            return objKey;
        }

        public Object getValue() {
            if (objValue == null) {
                if (value != null) {
                    objValue = toObject(value);
                } else {
                    FactoryImpl factory = FactoryImpl.getFactory(factoryName);
                    objValue = ((FactoryImpl.IGetAwareProxy) factory.getProxyByName(name)).get((key == null) ? getKey() : key);
                }
            }
            return objValue;
        }

        public Object setValue(Object newValue) {
            if (name == null) throw new UnsupportedOperationException();
            this.objValue = value;
            FactoryImpl factory = FactoryImpl.getFactory(factoryName);
            return ((FactoryImpl.MProxy) factory.getProxyByName(name)).put(getKey(), newValue);
        }

        public void setName(String factoryName, String name) {
            this.factoryName = factoryName;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Map.Entry key=" + getKey() + ", value=" + getValue();
        }
    }


    abstract class AbstractCall implements Call {
        private long id = -1;

        public AbstractCall() {
        }

        public long getId() {
            return id;
        }

        public void onDisconnect(final Address dead) {
        }

        public void redo() {
            removeCall(getId());
            id = -1;
            enqueueAndReturn(this);
        }

        public void setId(final long id) {
            this.id = id;
        }

        public void reset() {
            id = -1;
        }
    }

    public interface Call extends Processable {

        long getId();

        void handleResponse(Packet packet);

        void onDisconnect(Address dead);

        void setId(long id);
    }


    public interface Processable {
        void process();
    }

    interface RequestHandler {
        void handle(Request request);
    }

    abstract class MigrationAwareOperationHandler extends AbstractOperationHandler {

        @Override
        public void process(Packet packet) {
            if (isMigrating()) {
                packet.responseType = RESPONSE_REDO;
                sendResponse(packet);
            } else {
                Request remoteReq = new Request();
                remoteReq.setFromPacket(packet);
                remoteReq.local = false;
                handle(remoteReq);
                packet.returnToContainer();
            }
        }
    }


    abstract class TargetAwareOperationHandler extends MigrationAwareOperationHandler {

        abstract boolean isRightRemoteTarget(Packet packet);

        @Override
        public void process(Packet packet) {
            if (isMigrating()) {
                packet.responseType = RESPONSE_REDO;
                sendResponse(packet);
            } else if (!isRightRemoteTarget(packet)) {
            } else {
                Request remoteReq = new Request();
                remoteReq.setFromPacket(packet);
                remoteReq.local = false;
                handle(remoteReq);
                packet.returnToContainer();
            }
        }
    }

    abstract class AbstractOperationHandler implements PacketProcessor, RequestHandler {

        public void process(Packet packet) {
            Request request = new Request();
            request.setFromPacket(packet);
            request.local = false;
            handle(request);
            packet.returnToContainer();
            request.reset();
        }

        abstract void doOperation(Request request);

        public void handle(Request request) {
            doOperation(request);
            returnResponse(request);
        }

        public void returnResponse(Request request) {
            if (request.local) {
                final TargetAwareOp targetAwareOp = (TargetAwareOp) request.attachment;
                targetAwareOp.setResult(request.response);
            } else {
                Packet packet = obtainPacket();
                request.setPacket(packet);
                packet.operation = ClusterOperation.RESPONSE;
                packet.responseType = RESPONSE_SUCCESS;
                packet.longValue = request.longValue;
                if (request.value != null) {
                    doSet(request.value, packet.value);
                }
                if (request.response == OBJECT_REDO) {
                    packet.responseType = RESPONSE_REDO;
                } else if (request.response != null) {
                    if (request.response instanceof Boolean) {
                        if (request.response == Boolean.FALSE) {
                            packet.responseType = RESPONSE_FAILURE;
                        }
                    } else if (request.response instanceof Long) {
                        packet.longValue = (Long) request.response;
                    } else {
                        Data data;
                        if (request.response instanceof Data) {
                            data = (Data) request.response;
                        } else {
                            data = toData(request.response);
                        }
                        if (data != null && data.size() > 0) {
                            doSet(data, packet.value);
                        }
                    }
                }
                sendResponse(packet, request.caller);
                request.reset();
            }
        }

    }

    abstract class QueueBasedCall extends AbstractCall {
        final protected BlockingQueue responses;

        public QueueBasedCall() {
            this(true);
        }

        public QueueBasedCall(final boolean limited) {
            if (limited) {
                responses = new ArrayBlockingQueue(1);
            } else {
                responses = new LinkedBlockingQueue();
            }
        }

        public void handleBooleanNoneRedoResponse(final Packet packet) {
            removeCall(getId());
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                responses.add(Boolean.TRUE);
            } else {
                responses.add(Boolean.FALSE);
            }
        }

        @Override
        public void redo() {
            removeCall(getId());
            responses.clear();
            responses.add(OBJECT_REDO);
        }

        void handleObjectNoneRedoResponse(final Packet packet) {
            removeCall(getId());
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                final Data oldValue = doTake(packet.value);
                if (oldValue == null || oldValue.size() == 0) {
                    responses.add(OBJECT_NULL);
                } else {
                    responses.add(oldValue);
                }
            } else {
                throw new RuntimeException("responseType " + packet.responseType);
            }
        }
    }


    abstract class RequestBasedCall extends AbstractCall {
        final protected Request request = new Request();

        public boolean booleanCall(final ClusterOperation operation, final String name, final Object key,
                                   final Object value, final long timeout, final long recordId) {
            doOp(operation, name, key, value, timeout, recordId);
            return getResultAsBoolean();
        }

        public void doOp(final ClusterOperation operation, final String name, final Object key,
                         final Object value, final long timeout, final long recordId) {
            setLocal(operation, name, key, value, timeout, recordId);
            doOp();
        }

        public void reset() {
            super.reset();
            request.reset();
        }


        public boolean getResultAsBoolean() {
            try {
                final Object result = getResult();
                return !(result == OBJECT_NULL || result == null) && result == Boolean.TRUE;
            } catch (final Throwable e) {
                logger.log(Level.SEVERE, "getResultAsBoolean", e);
            } finally {
                afterGettingResult(request);
            }
            return false;
        }


        public Object getResultAsObject() {
            try {
                final Object result = getResult();

                if (result == OBJECT_NULL || result == null) {
                    return null;
                }
                if (result instanceof Data) {
                    final Data data = (Data) result;
                    if (data.size() == 0)
                        return null;
                    return toObject(data);
                }
                return result;
            } catch (final Throwable e) {
                logger.log(Level.SEVERE, "getResultAsObject", e);
            } finally {
                afterGettingResult(request);
            }
            return null;
        }

        protected void afterGettingResult(Request request) {
            request.reset();
        }

        public Object objectCall() {
            doOp();
            return getResultAsObject();
        }

        public Object objectCall(final ClusterOperation operation, final String name, final Object key,
                                 final Object value, final long timeout, final long recordId) {
            setLocal(operation, name, key, value, timeout, recordId);
            return objectCall();
        }

        public void setLocal(ClusterOperation operation, String name) {
            setLocal(operation, name, null, null, -1, -1);
        }

        public void setLocal(final ClusterOperation operation, final String name, final Object key,
                             final Object value, final long timeout, final long recordId) {
            Data keyData = null;
            Data valueData = null;
            if (key != null) {
                keyData = toData(key);
                if (keyData.size() == 0) {
                    throw new RuntimeException(name + " Key with zero-size " + operation);
                }
            }
            if (value != null) {
                valueData = toData(value);
            }
            request.setLocal(operation, name, keyData, valueData, -1, timeout, recordId, thisAddress);
            request.attachment = this;
        }

        abstract void doOp();

        abstract Object getResult();


        public String toString() {
            return RequestBasedCall.this.getClass().getSimpleName()
                    + " operation= " + ((request != null) ? request.operation : " unknown")
                    + " name= " + ((request != null) ? request.name : " unknown");
        }
    }

    public abstract class ResponseQueueCall extends RequestBasedCall {
        final protected BlockingQueue responses = new ArrayBlockingQueue(1);

        public ResponseQueueCall() {
        }

        @Override
        public void doOp() {
            responses.clear();
            enqueueAndReturn(ResponseQueueCall.this);
        }

        public void beforeRedo() {
        }

        @Override
        public Object getResult() {
            Object result = null;
            try {
                result = responses.take();
                if (result == OBJECT_REDO) {
                    request.redoCount++;
                    Thread.sleep(1000 * request.redoCount);
                    if (request.redoCount > 5) {
                        logger.log(Level.INFO, request.name + " Re-doing [" + request.redoCount + "] times! " + this);
                        logger.log(Level.INFO, "\t key= " + request.key + ", req.operation: " + request.operation);
                    }
                    beforeRedo();
                    doOp();
                    return getResult();
                }
            } catch (final Throwable e) {
                logger.log(Level.FINEST, "ResponseQueueCall.getResult()", e);
            }
            return result;
        }

        @Override
        public void redo() {
            removeCall(getId());
            responses.clear();
            setResult(OBJECT_REDO);
        }

        public void reset() {
            super.reset();
            responses.clear();
        }

        public void handleBooleanNoneRedoResponse(final Packet packet) {
            removeCall(getId());
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                setResult(Boolean.TRUE);
            } else {
                setResult(Boolean.FALSE);
            }
        }

        void handleLongNoneRedoResponse(final Packet packet) {
            removeCall(getId());
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                setResult(packet.longValue);
            } else {
                throw new RuntimeException("handleLongNoneRedoResponse.responseType "
                        + packet.responseType);
            }
        }

        void handleObjectNoneRedoResponse(final Packet packet) {
            removeCall(getId());
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                final Data oldValue = doTake(packet.value);
                if (oldValue == null || oldValue.size() == 0) {
                    setResult(OBJECT_NULL);
                } else {
                    setResult(oldValue);
                }
            } else {
                throw new RuntimeException("handleObjectNoneRedoResponse.responseType "
                        + packet.responseType);
            }
        }

        protected void setResult(final Object obj) {
            if (obj == null) {
                responses.add(OBJECT_NULL);
            } else {
                responses.add(obj);
            }
        }
    }

    public abstract class BooleanOp extends TargetAwareOp {
        @Override
        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
        }
    }

    abstract class LongOp extends TargetAwareOp {
        @Override
        void handleNoneRedoResponse(final Packet packet) {
            handleLongNoneRedoResponse(packet);
        }
    }

    public abstract class TargetAwareOp extends ResponseQueueCall {

        protected Address target = null;

        public TargetAwareOp() {
        }

        public void handleResponse(final Packet packet) {
            if (packet.responseType == RESPONSE_REDO) {
                redo();
            } else {
                handleNoneRedoResponse(packet);
            }
            packet.returnToContainer();
        }

        @Override
        public void onDisconnect(final Address dead) {
            if (dead.equals(target)) {
                target = null;
                redo();
            }
        }

        public void reset() {
            super.reset();
            target = null;
        }

        @Override
        public void beforeRedo() {
            logger.log(Level.FINEST, "BeforeRedo target " + target);
            super.beforeRedo();
        }

        public void process() {
            setTarget();
            if (target == null) {
                setResult(OBJECT_REDO);
            } else {
                if (target.equals(thisAddress)) {
                    doLocalOp();
                } else {
                    invoke();
                }
            }
        }

        protected void invoke() {
            addCall(TargetAwareOp.this);
            final Packet packet = obtainPacket();
            request.setPacket(packet);
            packet.callId = getId();
            final boolean sent = send(packet, target);
            if (!sent) {
                logger.log(Level.FINEST, TargetAwareOp.this + " Packet cannot be sent to " + target);
                packet.returnToContainer();
                redo();
            }
        }

        public void doLocalOp() {
            if (isMigrationAware() && isMigrating()) {
                setResult(OBJECT_REDO);
            } else {
                request.attachment = TargetAwareOp.this;
                request.local = true;
                ((RequestHandler) getPacketProcessor(request.operation)).handle(request);
            }
        }

        void handleNoneRedoResponse(final Packet packet) {
            handleObjectNoneRedoResponse(packet);
        }

        public abstract void setTarget();

        public boolean isMigrationAware() {
            return false;
        }
    }


    abstract class MultiCall {
        abstract TargetAwareOp createNewTargetAwareOp(Address target);

        /**
         * As MultiCall receives the responses from the target members
         * it will pass each response to the extending call so that it can
         * consume and checks if the call should continue.
         *
         * @param response response object from one of the targets
         * @return false if call is completed.
         */
        abstract boolean onResponse(Object response);

        void onComplete() {
        }

        void onRedo() {
        }

        void onCall() {
        }

        abstract Object returnResult();

        Object call() {
            try {
                onCall();
                //local call first
                TargetAwareOp localCall = createNewTargetAwareOp(thisAddress);
                localCall.doOp();
                Object result = localCall.getResultAsObject();
                if (result == OBJECT_REDO) {
                    onRedo();
                    Thread.sleep(2000);
                    return call();
                }
                if (onResponse(result)) {
                    Set<Member> members = node.getClusterImpl().getMembers();
                    List<TargetAwareOp> lsCalls = new ArrayList<TargetAwareOp>();
                    for (Member member : members) {
                        if (!member.localMember()) { // now other members
                            ClusterImpl.ClusterMember cMember = (ClusterImpl.ClusterMember) member;
                            TargetAwareOp targetAwareOp = createNewTargetAwareOp(cMember.getAddress());
                            targetAwareOp.doOp();
                            lsCalls.add(targetAwareOp);
                        }
                    }
                    for (TargetAwareOp call : lsCalls) {
                        result = call.getResultAsObject();
                        if (result == OBJECT_REDO) {
                            onRedo();
                            Thread.sleep(2000);
                            return call();
                        } else {
                            if (!onResponse(result)) {
                                break;
                            }
                        }
                    }
                    onComplete();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
            return returnResult();
        }
    }

    abstract class MigrationAwareTargettedCall extends TargetAwareOp {

        public void onDisconnect(final Address dead) {
            redo();
        }

        @Override
        public void setTarget() {
        }

        @Override
        public Object getResult() {
            Object result = null;
            try {
                result = responses.take();
            } catch (final Throwable e) {
                logger.log(Level.FINEST, "getResult()", e);
            }
            return result;
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    protected boolean isMigrating() {
        return false;
    }

    public static InstanceType getInstanceType(final String name) {
        if (name.startsWith("q:")) {
            return InstanceType.QUEUE;
        } else if (name.startsWith("t:")) {
            return InstanceType.TOPIC;
        } else if (name.startsWith("c:")) {
            return InstanceType.MAP;
        } else if (name.startsWith("m:")) {
            if (name.length() > 3) {
                final String typeStr = name.substring(2, 4);
                if ("s:".equals(typeStr)) {
                    return InstanceType.SET;
                } else if ("l:".equals(typeStr)) {
                    return InstanceType.LIST;
                } else if ("u:".equals(typeStr)) {
                    return InstanceType.MULTIMAP;
                }
            }
            return InstanceType.MAP;
        } else throw new RuntimeException("Unknown InstanceType " + name);
    }

    public long addCall(final Call call) {
        final long id = callIdGen++;
        call.setId(id);
        mapCalls.put(id, call);
        return id;
    }

    public void enqueueAndReturn(final Object obj) {
       node.clusterService.enqueueAndReturn(obj);
    }

    public Address getKeyOwner(final Data key) {
        return node.concurrentMapManager.getKeyOwner(key);
    }

    public Packet obtainPacket(final String name, final Object key,
                               final Object value, final ClusterOperation operation, final long timeout) {
        final Packet packet = obtainPacket();
        packet.set(name, operation, key, value);
        packet.timeout = timeout;
        return packet;
    }

    public Call removeCall(final Long id) {
        return mapCalls.remove(id);
    }

    public void registerPacketProcessor(ClusterOperation operation, PacketProcessor packetProcessor) {
        node.clusterService.registerPacketProcessor(operation, packetProcessor);
    }

    public PacketProcessor getPacketProcessor(ClusterOperation operation) {
        return node.clusterService.getPacketProcessor(operation);
    }

    public void returnScheduledAsBoolean(final Request request) {
        if (request.local) {
            final TargetAwareOp mop = (TargetAwareOp) request.attachment;
            mop.setResult(request.response);
        } else {
            final Packet packet = obtainPacket();
            request.setPacket(packet);
            if (request.response == Boolean.TRUE) {
                final boolean sent = sendResponse(packet, request.caller);
                log(request.local + " returning scheduled response " + sent);
            } else {
                sendResponseFailure(packet, request.caller);
            }
        }
    }

    public void returnScheduledAsSuccess(final Request request) {
        if (request.local) {
            final TargetAwareOp mop = (TargetAwareOp) request.attachment;
            mop.setResult(request.response);
        } else {
            final Packet packet = obtainPacket();
            request.setPacket(packet);
            final Object result = request.response;
            if (result != null) {
                if (result instanceof Data) {
                    final Data data = (Data) result;
                    if (data.size() > 0) {
                        doSet(data, packet.value);
                    }
                }
            }
            sendResponse(packet, request.caller);
        }
    }

    public void sendEvents(final int eventType, final String name, final Data key,
                           final Data value, final Map<Address, Boolean> mapListeners) {
        if (mapListeners != null) {
            final Set<Map.Entry<Address, Boolean>> listeners = mapListeners.entrySet();

            for (final Map.Entry<Address, Boolean> listener : listeners) {
                final Address address = listener.getKey();
                final boolean includeValue = listener.getValue();
                if (address.isThisAddress()) {
                    try {
                        enqueueEvent(eventType, name,
                                doHardCopy(key),
                                (includeValue) ? doHardCopy(value) : null,
                                address);
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    final Packet packet = ThreadContext.get().getPacketPool().obtain();
                    packet.reset();
                    packet.set(name, ClusterOperation.EVENT, key, (includeValue) ? value : null);
                    packet.longValue = eventType;
                    final boolean sent = send(packet, address);
                    if (!sent)
                        packet.returnToContainer();
                }
            }
        }
    }

    public void sendProcessableTo(final RemotelyProcessable rp, final Address address) {
        final Data value = toData(rp);
        final Packet packet = obtainPacket();
        packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
        final boolean sent = send(packet, address);
        if (!sent) {
            packet.returnToContainer();
        }
    }

    public void sendProcessableToAll(RemotelyProcessable rp, boolean processLocally) {
        if (processLocally) {
            rp.process();
        }
        Data value = toData(rp);
        for (MemberImpl member : lsMembers) {
            if (!member.localMember()) {
                Packet packet = obtainPacket();
                packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
                boolean sent = send(packet, member.getAddress());
                if (!sent) {
                    packet.returnToContainer();
                }
            }
        }
    }

    public final void executeLocally(final Runnable runnable) {
        node.executorManager.executeLocaly(runnable);
    }

    protected Address getMasterAddress() {
        return node.getMasterAddress();
    }

    protected final MemberImpl getNextMemberAfter(final Address address,
                                                  final boolean skipSuperClient, final int distance) {
        return getNextMemberAfter(lsMembers, address, skipSuperClient, distance);
    }

    protected final MemberImpl getNextMemberAfter(final List<MemberImpl> lsMembers,
                                                  final Address address, final boolean skipSuperClient, final int distance) {
        final int size = lsMembers.size();
        if (size <= 1)
            return null;
        int indexOfMember = -1;
        for (int i = 0; i < size; i++) {
            final MemberImpl member = lsMembers.get(i);
            if (member.getAddress().equals(address)) {
                indexOfMember = i;
            }
        }
        if (indexOfMember == -1)
            return null;
        indexOfMember++;
        int foundDistance = 0;
        for (int i = 0; i < size; i++) {
            final MemberImpl member = lsMembers.get((indexOfMember + i) % size);
            if (!(skipSuperClient && member.isSuperClient())) {
                foundDistance++;
            }
            if (foundDistance == distance)
                return member;
        }
        return null;
    }

    protected final MemberImpl getNextMemberBeforeSync(final Address address,
                                                       final boolean skipSuperClient, final int distance) {
        return getNextMemberAfter(node.clusterManager.getMembersBeforeSync(), address,
                skipSuperClient, distance);
    }

    protected final MemberImpl getPreviousMemberBefore(final Address address,
                                                       final boolean skipSuperClient, final int distance) {
        return getPreviousMemberBefore(lsMembers, address, skipSuperClient, distance);
    }

    protected final MemberImpl getPreviousMemberBefore(final List<MemberImpl> lsMembers,
                                                       final Address address, final boolean skipSuperClient, final int distance) {
        final int size = lsMembers.size();
        if (size <= 1)
            return null;
        int indexOfMember = -1;
        for (int i = 0; i < size; i++) {
            final MemberImpl member = lsMembers.get(i);
            if (member.getAddress().equals(address)) {
                indexOfMember = i;
            }
        }
        if (indexOfMember == -1)
            return null;
        indexOfMember += (size - 1);
        int foundDistance = 0;
        for (int i = 0; i < size; i++) {
            final MemberImpl member = lsMembers.get((indexOfMember - i) % size);
            if (!(skipSuperClient && member.isSuperClient())) {
                foundDistance++;
            }
            if (foundDistance == distance)
                return member;
        }
        return null;
    }

    protected final boolean isMaster() {
        return node.master();
    }

    protected final boolean isSuperClient() {
        return node.isSuperClient();
    }

    protected void log(final Object obj) {
        logger.log(Level.FINEST, obj.toString());
    }

    protected Packet obtainPacket() {
        return ThreadContext.get().getPacketPool().obtain();
    }

    protected final boolean send(final String name, final ClusterOperation operation, final DataSerializable ds,
                                 final Address address) {
        final Packet packet = obtainPacket();
        packet.set(name, operation, null, ds);
        final boolean sent = send(packet, address);
        if (!sent)
            packet.returnToContainer();
        return sent;
    }

    protected void sendRedoResponse(final Packet packet) {
        packet.responseType = RESPONSE_REDO;
        sendResponse(packet);
    }

    protected boolean sendResponse(final Packet packet) {
        packet.local = false;
        packet.operation = ClusterOperation.RESPONSE;
        if (packet.responseType == RESPONSE_NONE) {
            packet.responseType = RESPONSE_SUCCESS;
        }
        final boolean sent = send(packet, packet.conn);
        if (!sent) {
            packet.returnToContainer();
        }
        return sent;
    }

    protected boolean sendResponse(final Packet packet, final Address address) {
        packet.conn = node.connectionManager.getConnection(address);
        return sendResponse(packet);
    }

    protected boolean sendResponseFailure(final Packet packet) {
        packet.local = false;
        packet.operation = ClusterOperation.RESPONSE;
        packet.responseType = RESPONSE_FAILURE;
        final boolean sent = send(packet, packet.conn);
        if (!sent) {
            packet.returnToContainer();
        }
        return sent;
    }

    protected boolean sendResponseFailure(final Packet packet, final Address address) {
        packet.conn = node.connectionManager.getConnection(address);
        return sendResponseFailure(packet);
    }

    protected void throwCME(final Object key) {
        throw new ConcurrentModificationException("Another thread holds a lock for the key : "
                + key);
    }

    void enqueueEvent(final int eventType, final String name, final Data eventKey,
                      final Data eventValue, final Address from) {
        final EventTask eventTask = new EventTask(eventType, name, eventKey, eventValue);

        int eventQueueIndex;
        if (eventKey != null) {
            eventQueueIndex = Math.abs(eventKey.hashCode()) % EVENT_QUEUE_COUNT;
        } else {
            eventQueueIndex = Math.abs(from.hashCode()) % EVENT_QUEUE_COUNT;
        }
        final EventQueue eventQueue = eventQueues[eventQueueIndex];
        final int size = eventQueue.offerRunnable(eventTask);
        if (size == 1) executeLocally(eventQueue);
    }

    public static class EventQueue extends ConcurrentLinkedQueue<Runnable> implements Runnable {
        private AtomicInteger size = new AtomicInteger();

        public int offerRunnable(Runnable runnable) {
            offer(runnable);
            return size.incrementAndGet();
        }

        public void run() {
            while (true) {
                final Runnable eventTask = poll();
                if (eventTask != null) {
                    eventTask.run();
                    size.decrementAndGet();
                } else {
                    return;
                }
            }
        }
    }


    class EventTask extends EntryEvent implements Runnable {
        protected final Data dataKey;

        protected final Data dataValue;

        public EventTask(final int eventType, final String name, final Data dataKey,
                         final Data dataValue) {
            super(name, eventType, null, null);
            this.dataKey = dataKey;
            this.dataValue = dataValue;
        }

        public void run() {
            try {
                if (dataKey != null) {
                    key = toObject(dataKey);
                }
                if (dataValue != null) {
                    value = toObject(dataValue);
                } else if (collection) {
                    value = key;
                }
                node.listenerManager.callListeners(this);
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }
    }


    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Data value) {
        fireMapEvent(mapListeners, name, eventType, null, value, null);

    }

    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Data key, final Data value, Map<Address, Boolean> keyListeners) {
        try {
            // logger.log(Level.FINEST,eventType + " FireMapEvent " + record);
            Map<Address, Boolean> mapTargetListeners = null;
            if (keyListeners != null) {
                mapTargetListeners = new HashMap<Address, Boolean>(keyListeners);
            }

            if (mapListeners != null && mapListeners.size() > 0) {
                if (mapTargetListeners == null) {
                    mapTargetListeners = new HashMap<Address, Boolean>(mapListeners);
                } else {
                    final Set<Map.Entry<Address, Boolean>> entries = mapListeners.entrySet();
                    for (final Map.Entry<Address, Boolean> entry : entries) {
                        if (mapTargetListeners.containsKey(entry.getKey())) {
                            if (entry.getValue()) {
                                mapTargetListeners.put(entry.getKey(), entry.getValue());
                            }
                        } else
                            mapTargetListeners.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            if (mapTargetListeners == null || mapTargetListeners.size() == 0) {
                return;
            }
            sendEvents(eventType, name, doHardCopy(key), doHardCopy(value), mapTargetListeners);
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    MemberImpl getMember(final Address address) {
        return node.clusterManager.getMember(address);
    }


    void handleListenerRegisterations(final boolean add, final String name, final Data key,
                                      final Address address, final boolean includeValue) {
        if (name.startsWith("q:")) {
            node.blockingQueueManager.handleListenerRegisterations(add, name, key, address,
                    includeValue);
        } else if (name.startsWith("t:")) {
            node.topicManager.handleListenerRegisterations(add, name, key, address, includeValue);
        } else {
            node.concurrentMapManager.handleListenerRegisterations(add, name, key, address,
                    includeValue);
        }
    }

    public final void handleResponse(final Packet packetResponse) {
        final Call call = mapCalls.get(packetResponse.callId);
        if (call != null) {
            call.handleResponse(packetResponse);
        } else {
            logger.log(Level.FINEST, packetResponse.operation + " No call for callId " + packetResponse.callId);
            packetResponse.returnToContainer();
        }
    }

    final boolean send(final Packet packet, final Address address) {
        if (address == null) return false;
        final Connection conn = node.connectionManager.getConnection(address);
        return conn != null && conn.live() && writePacket(conn, packet);
    }

    protected final boolean send(final Packet packet, final Connection conn) {
        return conn != null && conn.live() && writePacket(conn, packet);
    }

    protected final boolean sendOrReleasePacket(final Packet packet, final Connection conn) {
        if (conn != null && conn.live()) {
            if (writePacket(conn, packet)) {
                return true;
            }
        }
        packet.returnToContainer();
        return false;
    }

    private boolean writePacket(final Connection conn, final Packet packet) {
        final MemberImpl memberImpl = getMember(conn.getEndPoint());
        if (memberImpl != null) {
            memberImpl.didWrite();
        }
        packet.currentCallCount = mapCalls.size();
        conn.getWriteHandler().enqueuePacket(packet);
        return true;
    }

    public interface PacketProcessor {
        void process(Packet packet);
    }
}
