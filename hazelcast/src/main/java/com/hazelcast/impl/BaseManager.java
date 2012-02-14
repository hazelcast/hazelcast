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

import com.hazelcast.cluster.RemotelyProcessable;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Member;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.base.*;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.ResponseQueueFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.hazelcast.core.Instance.InstanceType;
import static com.hazelcast.impl.Constants.Objects.OBJECT_NULL;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.ResponseTypes.*;
import static com.hazelcast.impl.base.CallStateService.Level.CS_INFO;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public abstract class BaseManager {

    protected final List<MemberImpl> lsMembers;

    protected final Map<Address, MemberImpl> mapMembers;

    protected final Queue<Packet> qServiceThreadPacketCache;

    protected final Map<Long, Call> mapCalls;

    protected final AtomicLong localIdGen;

    protected final Address thisAddress;

    protected final MemberImpl thisMember;

    protected final Node node;

    protected final ILogger logger;

    protected final long redoWaitMillis;

    protected BaseManager(Node node) {
        this.node = node;
        lsMembers = node.baseVariables.lsMembers;
        mapMembers = node.baseVariables.mapMembers;
        mapCalls = node.baseVariables.mapCalls;
        thisAddress = node.baseVariables.thisAddress;
        thisMember = node.baseVariables.thisMember;
        qServiceThreadPacketCache = node.baseVariables.qServiceThreadPacketCache;
        this.localIdGen = node.baseVariables.localIdGen;
        this.logger = node.getLogger(this.getClass().getName());
        this.redoWaitMillis = node.getGroupProperties().REDO_WAIT_MILLIS.getLong();
    }

    public List<MemberImpl> getMembers() {
        return lsMembers;
    }

    public Address getThisAddress() {
        return thisAddress;
    }

    public Node getNode() {
        return node;
    }

    public static MapEntry createSimpleMapEntry(final FactoryImpl factory, final String name, final Object key, final Object value) {
        return new MapEntry() {
            public Object getKey() {
                return key;
            }

            public Object getValue() {
                return value;
            }

            public Object setValue(Object newValue) {
                return ((MProxy) factory.getOrCreateProxyByName(name)).put(key, newValue);
            }

            public long getCost() {
                return 0;
            }

            public long getCreationTime() {
                return 0;
            }

            public long getExpirationTime() {
                return 0;
            }

            public int getHits() {
                return 0;
            }

            public long getLastAccessTime() {
                return 0;
            }

            public long getLastStoredTime() {
                return 0;
            }

            public long getLastUpdateTime() {
                return 0;
            }

            public long getVersion() {
                return 0;
            }

            public boolean isValid() {
                return false;
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

    abstract class AbstractCall implements Call, CallStateAware {
        protected long callId = -1;
        protected long firstEnqueueTime = -1;
        protected int enqueueCount = 0;
        protected CallState callState = null;

        public AbstractCall() {
            initCall();
        }

        public CallState getCallState() {
            return callState;
        }

        protected void initCall() {
            callId = localIdGen.incrementAndGet();
            int threadId = ThreadContext.get().getThreadId();
            callState = node.getCallStateService().newCallState(callId, thisAddress, threadId);
        }

        public long getCallId() {
            return callId;
        }

        public void onDisconnect(final Address dead) {
        }

        public void onEnqueue() {
            if (firstEnqueueTime == -1) {
                firstEnqueueTime = System.currentTimeMillis();
            }
            enqueueCount++;
        }

        public void redo() {
            removeRemoteCall(getCallId());
            enqueueCall(this);
        }

        public void setCallId(long callId) {
            this.callId = callId;
        }

        public void reset() {
            initCall();
            firstEnqueueTime = -1;
            enqueueCount = 0;
        }

        public int getEnqueueCount() {
            return enqueueCount;
        }

        protected int getDurationSeconds() {
            return (int) (System.currentTimeMillis() - firstEnqueueTime) / 1000;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{[" +
                    "" + getCallId() +
                    "], duration=" + (+getDurationSeconds()) +
                    "sn., enqueueCount=" + getEnqueueCount() +
                    '}';
        }
    }

    abstract class MigrationAwareOperationHandler extends AbstractOperationHandler {
        @Override
        public void process(Packet packet) {
            super.processMigrationAware(packet);
        }
    }

    abstract class TargetAwareOperationHandler extends MigrationAwareOperationHandler {

        abstract boolean isRightRemoteTarget(Request request);

        @Override
        public void process(Packet packet) {
            Request remoteReq = Request.copy(packet);
            boolean isMigrating = isMigrating(remoteReq);
            boolean rightRemoteTarget = isRightRemoteTarget(remoteReq);
            CallStateService css = node.getCallStateService();
            if (css.shouldLog(CS_INFO)) {
                css.logObject(remoteReq, CS_INFO, "IsMigrating " + isMigrating);
                css.logObject(remoteReq, CS_INFO, "RightRemoteTarget " + rightRemoteTarget);
            }
            if (isMigrating || !rightRemoteTarget) {
                remoteReq.clearForResponse();
                remoteReq.response = OBJECT_REDO;
                returnResponse(remoteReq);
            } else {
                if (css.shouldLog(CS_INFO)) {
                    css.logObject(remoteReq, CS_INFO, "handle");
                }
                handle(remoteReq);
            }
            releasePacket(packet);
        }
    }

    public abstract class ResponsiveOperationHandler implements PacketProcessor, RequestHandler {

        public void process(Packet packet) {
            processSimple(packet);
        }

        public void processSimple(Packet packet) {
            Request request = Request.copy(packet);
            handle(request);
            releasePacket(packet);
        }

        public void processMigrationAware(Packet packet) {
            Request remoteReq = Request.copy(packet);
            if (isMigrating(remoteReq)) {
                remoteReq.clearForResponse();
                remoteReq.response = OBJECT_REDO;
                returnResponse(remoteReq);
            } else {
                handle(remoteReq);
            }
            releasePacket(packet);
        }
    }

    public class ReturnResponseProcess implements Processable {
        private final Request request;

        public ReturnResponseProcess(Request request) {
            this.request = request;
        }

        public void process() {
            returnResponse(request);
        }
    }

    public boolean returnRedoResponse(Request request) {
        request.response = OBJECT_REDO;
        return returnResponse(request, null);
    }

    public boolean returnResponse(Request request) {
        return returnResponse(request, null);
    }

    public boolean returnResponse(Request request, Connection conn) {
        CallStateService css = node.getCallStateService();
        if (css.shouldLog(CS_INFO)) {
            css.logObject(request, CS_INFO, "ReturnResponse " + conn);
        }
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
                packet.setValue(request.value);
            }
            if (request.response == OBJECT_REDO) {
                packet.lockAddress = null;
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
                        packet.setValue(data);
                    }
                }
            }
            if (conn != null) {
                conn.getWriteHandler().enqueueSocketWritable(packet);
            } else {
                return sendResponse(packet, request.caller);
            }
        }
        return true;
    }

    abstract class AbstractOperationHandler extends ResponsiveOperationHandler {

        public void process(Packet packet) {
            processSimple(packet);
        }

        abstract void doOperation(Request request);

        public void handle(Request request) {
            doOperation(request);
            returnResponse(request);
        }
    }

    abstract class RequestBasedCall extends AbstractCall {
        final protected Request request = new Request();

        protected RequestBasedCall() {
            request.callState = callState;
        }

        public boolean booleanCall(final ClusterOperation operation, final String name, final Object key,
                                   final Object value, final long timeout, final long recordId) {
            setLocal(operation, name, key, value, timeout, recordId);
            request.setBooleanRequest();
            doOp();
            return getResultAsBoolean();
        }

        public void reset() {
            super.reset();
            request.callState = callState;
        }

        public void clearRequest() {
            request.response = null;
            request.value = null;
        }

        public boolean getResultAsBoolean() {
            Object resultObj = getResult();
            if (resultObj instanceof Data) {
                resultObj = toObject((Data) resultObj);
                if (resultObj instanceof AddressAwareException) {
                    rethrowException(request.operation, (AddressAwareException) resultObj);
                }
            }
            boolean result = Boolean.TRUE.equals(resultObj);
            afterGettingResult(request);
            return result;
        }

        public Object getResultAsObject() {
            return getResultAsObject(true);
        }

        public Object getResultAsObject(boolean force) {
            Object result = getResult();
            if (result == OBJECT_NULL || result == null) {
                result = null;
            } else {
                if (result instanceof Data) {
                    final Data data = (Data) result;
                    if (ThreadContext.get().isClient() && force) {
                        result = data;
                    } else {
                        if (data.size() == 0) {
                            result = null;
                        } else {
                            result = toObject(data);
                        }
                    }
                }
            }
            afterGettingResult(request);
            return result;
        }

        public Object getResultAsIs() {
            Object result = getResult();
            if (result == OBJECT_NULL || result == null) {
                result = null;
            } else {
                return result;
            }
            afterGettingResult(request);
            return result;
        }

        protected void afterGettingResult(Request request) {
        }

        public Object objectCall() {
            request.setObjectRequest();
            doOp();
            return getResultAsObject();
        }

        public Object objectCall(final ClusterOperation operation, final String name, final Object key,
                                 final Object value, final long timeout, final long ttl) {
            setLocal(operation, name, key, value, timeout, ttl);
            return objectCall();
        }

        public void setLocal(ClusterOperation operation, String name) {
            setLocal(operation, name, null, null, -1, -1);
        }

        public void setLocal(final ClusterOperation operation, final String name, final Object key,
                             final Object value, final long timeout, final long ttl) {
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
            request.setLocal(operation, name, keyData, valueData, -1, timeout, ttl, thisAddress);
            request.attachment = this;
        }

        abstract void doOp();

        abstract Object getResult();

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{[" +
                    "" + getCallId() +
                    "], duration=" + (+getDurationSeconds()) +
                    "sn., enqueueCount=" + getEnqueueCount() +
                    ", " + request +
                    '}';
        }
    }

    protected void handleInterruptedException() {
        if (node.factory.restarted) {
            throw new RuntimeException();
        } else {
            throw new RuntimeInterruptedException(Thread.currentThread().toString() + " is interrupted.");
        }
    }

    public abstract class ResponseQueueCall extends RequestBasedCall {

        private final BlockingQueue<Object> responses = ResponseQueueFactory.newResponseQueue();

        public ResponseQueueCall() {
        }

        @Override
        public void doOp() {
            responses.clear();
            enqueueCall(ResponseQueueCall.this);
        }

        public void beforeRedo() {
            request.beforeRedo();
            node.checkNodeState();
        }

        public Object getResult(long time, TimeUnit unit) throws InterruptedException {
            return responses.poll(time, unit);
        }

        public boolean getResultAsBoolean(int timeoutSeconds) {
            Object resultObj = null;
            try {
                resultObj = getResult(timeoutSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                handleInterruptedException();
            }
            boolean result = Boolean.TRUE.equals(resultObj);
            afterGettingResult(request);
            return result;
        }

        protected void onStillWaiting() {
        }

        public Object waitAndGetResult() {
            while (true) {
                try {
                    Object obj = responses.poll(10, TimeUnit.SECONDS);
                    if (obj != null) {
                        return obj;
                    }
                    if (node.isActive()) {
                        logger.log(Level.FINEST, "Still no response! " + request);
                    }
                    node.checkNodeState();
                    if (Thread.interrupted()) {
                        handleInterruptedException();
                    }
                    onStillWaiting();
                } catch (InterruptedException e) {
                    handleInterruptedException();
                }
            }
        }

        @Override
        public Object getResult() {
            return getRedoAwareResult();
        }

        protected final Object getRedoAwareResult() {
            for (; ; ) {
                Object result = waitAndGetResult();
                if (Thread.interrupted()) {
                    handleInterruptedException();
                }
                if (result == OBJECT_REDO) {
                    request.redoCount++;
//                    System.out.println(ResponseQueueCall.this + " request block id " + request.blockId);
                    if (request.redoCount > 19 && (request.redoCount % 10 == 0)) {
                        final CountDownLatch l = new CountDownLatch(1);
                        final Request reqCopy = request.hardCopy();
                        reqCopy.redoCount = request.redoCount;
                        final Address targetCopy = getTarget();
                        if (!thisAddress.equals(targetCopy)) {
                            enqueueAndReturn(new Processable() {
                                public void process() {
                                    Connection targetConnection = null;
                                    MemberImpl targetMember = null;
                                    Object key = toObject(reqCopy.key);
//                                    System.out.println("BLOCK ID " + reqCopy.blockId);
                                    PartitionInfo block = (reqCopy.key == null) ? null : node.concurrentMapManager.getPartitionInfo(reqCopy.blockId);
                                    if (targetCopy != null) {
                                        targetMember = getMember(targetCopy);
                                        targetConnection = node.connectionManager.getConnection(targetCopy);
                                        if (targetMember != null) {
                                            if (!lsMembers.contains(targetMember)) {
                                                logger.log(Level.SEVERE, targetMember + " is not in member list!");
                                            }
                                        }
                                    }
                                    final String msg =
                                            "======= " + reqCopy.callId + ": " + reqCopy.operation + " ======== " +
                                                    "\n\t" +
                                                    "thisAddress= " + thisAddress + ", target= " + targetCopy +
                                                    "\n\t" +
                                                    "targetMember= " + targetMember + ", targetConn=" + targetConnection + ", targetBlock=" + block +
                                                    "\n\t" +
                                                    key + " Re-doing [" + reqCopy.redoCount + "] times! " +
                                                    reqCopy.name + " : " + toObject(reqCopy.value);
                                    logger.log(Level.INFO, msg);
                                    l.countDown();
                                }
                            });
                        } else {
                            final String msg =
                                    "======= " + reqCopy.callId + ": " + reqCopy.operation + " ======== " +
                                            "\n\t" +
                                            "thisAddress= " + thisAddress + ", target= " + targetCopy +
                                            "\n\t" +
                                            " Re-doing [" + reqCopy.redoCount + "] times! " +
                                            reqCopy.name + " : " + toObject(reqCopy.key) + "=" + toObject(reqCopy.value);
                            logger.log(Level.WARNING, msg);
                            l.countDown();
                        }
                        try {
                            l.await();
                        } catch (InterruptedException e) {
                            handleInterruptedException();
                        }
                    }
                    try {
                        //noinspection BusyWait
                        Thread.sleep(redoWaitMillis);
                    } catch (InterruptedException e) {
                        handleInterruptedException();
                    }
                    beforeRedo();
                    doOp();
                    continue;
                }
                return result;
            }
        }

        protected Address getTarget() {
            return thisAddress;
        }

        @Override
        public void redo() {
            removeRemoteCall(getCallId());
            responses.clear();
            setResult(OBJECT_REDO);
        }

        public void reset() {
            if (getCallId() != -1) {
                removeRemoteCall(getCallId());
            }
            super.reset();
        }

        private void handleBooleanNoneRedoResponse(final Packet packet) {
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                setResult(Boolean.TRUE);
            } else {
                setResult(Boolean.FALSE);
            }
        }

        private void handleLongNoneRedoResponse(final Packet packet) {
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                setResult(packet.longValue);
            } else {
                throw new RuntimeException("handleLongNoneRedoResponse.responseType "
                        + packet.responseType);
            }
        }

        private void handleObjectNoneRedoResponse(final Packet packet) {
            if (packet.responseType == Constants.ResponseTypes.RESPONSE_SUCCESS) {
                final Data oldValue = packet.getValueData();
                if (oldValue == null || oldValue.size() == 0) {
                    setResult(OBJECT_NULL);
                } else {
                    setResult(oldValue);
                }
            } else {
                throw new RuntimeException(request.operation + " handleObjectNoneRedoResponse.responseType "
                        + packet.responseType);
            }
        }

        protected void handleNoneRedoResponse(final Packet packet) {
            removeRemoteCall(getCallId());
            if (request.isBooleanRequest()) {
                handleBooleanNoneRedoResponse(packet);
            } else if (request.isLongRequest()) {
                handleLongNoneRedoResponse(packet);
            } else if (request.isObjectRequest()) {
                handleObjectNoneRedoResponse(packet);
            } else {
                throw new RuntimeException(request.operation + " Unknown request.responseType. " + request.responseType);
            }
        }

        protected void setResult(final Object obj) {
            responses.offer(obj == null ? OBJECT_NULL : obj);
        }
    }

    public abstract class ConnectionAwareOp extends ResponseQueueCall {

        final protected Connection targetConnection;

        public ConnectionAwareOp(Connection targetConnection) {
            this.targetConnection = targetConnection;
        }

        public void handleResponse(final Packet packet) {
            if (packet.responseType == RESPONSE_REDO) {
                redo();
            } else {
                handleNoneRedoResponse(packet);
            }
            releasePacket(packet);
        }

        @Override
        public void onDisconnect(final Address dead) {
        }

        public void reset() {
            super.reset();
        }

        @Override
        public void beforeRedo() {
            logger.log(Level.FINEST, request.operation + " BeforeRedo target " + targetConnection);
            super.beforeRedo();
        }

        public void process() {
            invoke();
        }

        protected void invoke() {
            addRemoteCall(ConnectionAwareOp.this);
            final Packet packet = obtainPacket();
            request.setPacket(packet);
            packet.callId = getCallId();
            request.callId = getCallId();
            final boolean sent = send(packet, targetConnection);
            if (!sent) {
                logger.log(Level.FINEST, ConnectionAwareOp.this + " Packet cannot be sent to " + targetConnection);
                releasePacket(packet);
                packetNotSent();
            }
        }

        protected void packetNotSent() {
            setResult(new IOException("Connection is lost!"));
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{[" +
                    "" + getCallId() +
                    "], firstEnqueue=" + (System.currentTimeMillis() - firstEnqueueTime) / 1000 +
                    "sn., enqueueCount=" + enqueueCount +
                    ", " + request +
                    ", target=" + getTarget() +
                    '}';
        }
    }

    public abstract class TargetAwareOp extends ResponseQueueCall {

        protected Address target = null;
        protected Connection targetConnection = null;

        public TargetAwareOp() {
        }

        public void handleResponse(final Packet packet) {
            if (packet.responseType == RESPONSE_REDO) {
                redo();
            } else {
                handleNoneRedoResponse(packet);
            }
            doReleasePacket(packet);
        }

        protected void doReleasePacket(Packet packet) {
            releasePacket(packet);
        }

        protected Packet doObtainPacket() {
            return obtainPacket();
        }

        @Override
        protected void onStillWaiting() {
            enqueueAndReturn(new Processable() {
                public void process() {
                    if (targetConnection != null && !targetConnection.live()) {
                        redo();
                    }
                }
            });
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
            targetConnection = null;
        }

        @Override
        public void beforeRedo() {
            logger.log(Level.FINEST, request.operation + " BeforeRedo target " + target);
            super.beforeRedo();
        }

        public void process() {
            request.caller = thisAddress;
            setTarget();
            if (target == null) {
                setResult(OBJECT_REDO);
            } else {
                if (target.equals(thisAddress)) {
                    request.callId = getCallId();
                    doLocalOp();
                } else {
                    invoke();
                }
            }
        }

        protected boolean memberOnly() {
            return true;
        }

        protected void memberDoesNotExist() {
            setResult(OBJECT_REDO);
        }

        protected void invoke() {
            if (memberOnly() && getMember(target) == null) {
                memberDoesNotExist();
            } else {
                addRemoteCall(TargetAwareOp.this);
                final Packet packet = doObtainPacket();
                request.setPacket(packet);
                packet.callId = getCallId();
                request.callId = getCallId();
                targetConnection = node.connectionManager.getOrConnect(target);
                boolean sent = send(packet, targetConnection);
                if (!sent) {
                    targetConnection = null;
                    logger.log(Level.FINEST, TargetAwareOp.this + " Packet cannot be sent to " + target);
                    releasePacket(packet);
                    packetNotSent();
                }
            }
        }

        protected void packetNotSent() {
            redo();
        }

        public void doLocalOp() {
            if (isMigrationAware() && isMigrating(request)) {
                setResult(OBJECT_REDO);
            } else {
                request.attachment = TargetAwareOp.this;
                request.local = true;
                ((RequestHandler) getPacketProcessor(request.operation)).handle(request);
            }
        }

        public abstract void setTarget();

        @Override
        public Address getTarget() {
            return target;
        }

        public boolean isMigrationAware() {
            return false;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{[" +
                    "" + getCallId() +
                    "], firstEnqueue=" + (System.currentTimeMillis() - firstEnqueueTime) / 1000 +
                    "sn., enqueueCount=" + enqueueCount +
                    ", " + request +
                    ", target=" + getTarget() +
                    '}';
        }
    }

    abstract class MultiCall<T> {
        int redoCount = 0;
        boolean excludeSuperClient = true;

        private void logRedo(SubCall subCall) {
            redoCount++;
            if (redoCount >= 20 && redoCount % 20 == 0) {
                logger.log(Level.WARNING, buildRedoLog(subCall));
            }
        }

        private String buildRedoLog(SubCall subCall) {
            StringBuilder s = new StringBuilder();
            s.append("=========== REDO LOG =========== ");
            s.append(MultiCall.this.getClass().getName());
            s.append(" Redoing ");
            s.append(subCall.request);
            s.append("\n");
            s.append(node.getClusterImpl());
            s.append("\n============================== ");
            return s.toString();
        }

        abstract SubCall createNewTargetAwareOp(Address target);

        /**
         * As MultiCall receives the responses from the target members
         * it will pass each response to the extending call so that it can
         * consume and check if the call should continue.
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

        protected Address getFirstAddressToMakeCall() {
            return thisAddress;
        }

        T call() {
            try {
                node.checkNodeState();
                onCall();
                //local call first
                Object result = null;
                boolean excludeThisMember = excludeSuperClient && thisMember.isSuperClient();
                if (!excludeThisMember) {
                    SubCall localCall = createNewTargetAwareOp(getFirstAddressToMakeCall());
                    localCall.doOp();
                    result = localCall.getResultAsObject();
                    if (result == OBJECT_REDO) {
                        logRedo(localCall);
                        onRedo();
                        Thread.sleep(redoWaitMillis);
                        return call();
                    }
                }
                // now other members
                boolean runOnOtherMembers = excludeThisMember || onResponse(result);
                if (runOnOtherMembers) {
                    Set<Member> members = node.getClusterImpl().getMembers();
                    List<SubCall> lsCalls = new ArrayList<SubCall>();
                    for (Member member : members) {
                        MemberImpl cMember = (MemberImpl) member;
                        boolean excludeMember = excludeSuperClient && cMember.isSuperClient();
                        if (!excludeMember && !cMember.getAddress().equals(getFirstAddressToMakeCall())) {
                            SubCall subCall = createNewTargetAwareOp(cMember.getAddress());
                            subCall.doOp();
                            lsCalls.add(subCall);
                        }
                    }
                    for (SubCall call : lsCalls) {
                        result = call.getResultAsObject();
                        if (result == OBJECT_REDO) {
                            logRedo(call);
                            onRedo();
                            //noinspection BusyWait
                            Thread.sleep(redoWaitMillis);
                            return call();
                        } else {
                            if (!onResponse(result)) {
                                break;
                            }
                        }
                    }
                    onComplete();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return (T) returnResult();
        }
    }

    abstract class SubCall extends TargetAwareOp {

        public SubCall(final Address target) {
            this.target = target;
            if (target == null) {
                throw new IllegalArgumentException("SubCall target cannot be " + target);
            }
        }

        public void onDisconnect(final Address dead) {
            removeRemoteCall(getCallId());
            setResult(OBJECT_REDO);
        }

        @Override
        public void setTarget() {
        }

        @Override
        protected void memberDoesNotExist() {
            setResult(OBJECT_REDO);
        }

        @Override
        public Object getResult() {
            // we don't want to REDO automatically
            // MultiCall will control this
            return waitAndGetResult();
        }

        @Override
        public boolean isMigrationAware() {
            return false;
        }
    }

    protected boolean isMigrating(Request req) {
        return false;
    }

    public static InstanceType getInstanceType(final String name) {
        if (name.startsWith(Prefix.ATOMIC_NUMBER)) {
            return InstanceType.ATOMIC_NUMBER;
        } else if (name.startsWith(Prefix.COUNT_DOWN_LATCH)) {
            return InstanceType.COUNT_DOWN_LATCH;
        } else if (name.startsWith(Prefix.IDGEN)) {
            return InstanceType.ID_GENERATOR;
        } else if (name.startsWith(Prefix.AS_LIST)) {
            return InstanceType.LIST;
        } else if (name.startsWith(Prefix.MAP)) {
            return InstanceType.MAP;
        } else if (name.startsWith(Prefix.MULTIMAP)) {
            return InstanceType.MULTIMAP;
        } else if (name.startsWith(Prefix.QUEUE)) {
            return InstanceType.QUEUE;
        } else if (name.startsWith(Prefix.SEMAPHORE)) {
            return InstanceType.SEMAPHORE;
        } else if (name.startsWith(Prefix.SET)) {
            return InstanceType.SET;
        } else if (name.startsWith(Prefix.TOPIC)) {
            return InstanceType.TOPIC;
        } else {
            throw new RuntimeException("Unknown InstanceType " + name);
        }
    }

    public void enqueueCall(Call call) {
        call.onEnqueue();
        enqueueAndReturn(call);
    }

    public void enqueueAndReturn(final Processable obj) {
        node.clusterService.enqueueAndReturn(obj);
    }

    public boolean enqueueAndWait(final Processable processable, final int seconds) {
        return node.clusterService.enqueueAndWait(processable, seconds);
    }

    public void enqueueAndWait(final Processable processable) {
        node.clusterService.enqueueAndWait(processable);
    }

    public Packet obtainPacket(String name, Object key, Object value,
                               ClusterOperation operation, long timeout) {
        final Packet packet = obtainPacket();
        packet.set(name, operation, key, value);
        packet.timeout = timeout;
        return packet;
    }

    public Call getRemoteCall(long id) {
        return mapCalls.get(id);
    }

    public void addRemoteCall(Call call) {
        mapCalls.put(call.getCallId(), call);
    }

    public Call removeRemoteCall(long id) {
        return mapCalls.remove(id);
    }

    public void registerPacketProcessor(ClusterOperation operation, PacketProcessor packetProcessor) {
        node.clusterService.registerPacketProcessor(operation, packetProcessor);
    }

    public PacketProcessor getPacketProcessor(ClusterOperation operation) {
        return node.clusterService.getPacketProcessor(operation);
    }

    public void sendEvents(int eventType, String name, Data key, Data value, Map<Address, Boolean> mapListeners, Address callerAddress) {
        if (mapListeners != null) {
            checkServiceThread();
            final Set<Map.Entry<Address, Boolean>> listeners = mapListeners.entrySet();
            for (final Map.Entry<Address, Boolean> listener : listeners) {
                final Address toAddress = listener.getKey();
                final boolean includeValue = listener.getValue();
                if (toAddress.equals(thisAddress)) {
                    // During local event listener calls, no need to check for include value flag.
                    // ListenerManager checks internally include value flag for each listener.
                    // By this way we can handle scenario of successively registered 
                    // a LocalEntryListener (which sets implicitly include-value to true) 
                    // and an EntryListener whose include-value is false. 
                    enqueueEvent(eventType, name, key, /*(includeValue) ? value : null*/ value, callerAddress, true);
                } else {
                    final Packet packet = obtainPacket();
                    packet.set(name, ClusterOperation.EVENT, key, (includeValue) ? value : null);
                    packet.lockAddress = callerAddress;
                    packet.longValue = eventType;
                    final boolean sent = send(packet, toAddress);
                    if (!sent)
                        releasePacket(packet);
                }
            }
        }
    }

    public Packet createRemotelyProcessablePacket(RemotelyProcessable rp) {
        Data value = ThreadContext.get().toData(rp);
        Packet packet = obtainPacket();
        packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
        return packet;
    }

    public void sendProcessableTo(RemotelyProcessable rp, Connection conn) {
        Packet packet = createRemotelyProcessablePacket(rp);
        boolean sent = send(packet, conn);
        if (!sent) {
            releasePacket(packet);
        }
    }

    public boolean sendProcessableTo(final RemotelyProcessable rp, final Address address) {
        final Data value = toData(rp);
        final Packet packet = obtainPacket();
        packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
        final boolean sent = send(packet, address);
        if (!sent) {
            releasePacket(packet);
        }
        return sent;
    }

    public void sendProcessableToAll(RemotelyProcessable rp, boolean processLocally) {
        if (processLocally) {
            rp.setNode(node);
            rp.process();
        }
        Data value = toData(rp);
        for (MemberImpl member : lsMembers) {
            if (!member.localMember()) {
                Packet packet = obtainPacket();
                packet.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, null, value);
                boolean sent = send(packet, member.getAddress());
                if (!sent) {
                    releasePacket(packet);
                }
            }
        }
    }

    public void executeLocally(Runnable runnable) {
        node.executorManager.executeLocally(runnable);
    }

    protected Address getMasterAddress() {
        return node.getMasterAddress();
    }

    protected MemberImpl getNextMemberAfter(final Address address,
                                            final boolean skipSuperClient,
                                            final int distance) {
        return getNextMemberAfter(lsMembers, address, skipSuperClient, distance);
    }

    protected MemberImpl getNextMemberAfter(final List<MemberImpl> lsMembers,
                                            final Address address,
                                            final boolean skipSuperClient,
                                            final int distance) {
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
        int foundDistance = 0;
        for (int i = indexOfMember; i < size + indexOfMember; i++) {
            final MemberImpl member = lsMembers.get((1 + i) % size);
            if (!(skipSuperClient && member.isSuperClient())) {
                foundDistance++;
            }
            if (foundDistance == distance) {
                return member;
            }
        }
        return null;
    }

    protected boolean isMaster() {
        return node.isMaster();
    }

    protected boolean isActive() {
        return node.isActive();
    }

    protected boolean isLiteMember() {
        return node.isLiteMember();
    }

    protected Packet obtainPacket() {
        if (Thread.currentThread() != node.serviceThread) {
            return new Packet();
        } else {
            Packet packet = node.serviceThreadPacketQueue.poll();
            if (packet == null) {
                packet = new Packet();
            } else {
                packet.reset();
            }
            return packet;
        }
    }

    protected boolean releasePacket(Packet packet) {
        return Thread.currentThread() == node.serviceThread && node.serviceThreadPacketQueue.offer(packet);
    }

    protected boolean sendResponse(final Packet packet) {
        packet.operation = ClusterOperation.RESPONSE;
        if (packet.responseType == RESPONSE_NONE) {
            packet.responseType = RESPONSE_SUCCESS;
        } else if (packet.responseType == RESPONSE_REDO) {
            packet.lockAddress = null;
        }
        final boolean sent = send(packet, packet.conn);
        if (!sent) {
            releasePacket(packet);
        }
        return sent;
    }

    protected boolean sendResponse(final Packet packet, final Address address) {
        packet.conn = node.connectionManager.getOrConnect(address);
        return sendResponse(packet);
    }

    protected boolean sendResponseFailure(final Packet packet) {
        packet.operation = ClusterOperation.RESPONSE;
        packet.responseType = RESPONSE_FAILURE;
        final boolean sent = send(packet, packet.conn);
        if (!sent) {
            releasePacket(packet);
        }
        return sent;
    }

    protected boolean sendResponseFailure(final Packet packet, final Address address) {
        packet.conn = node.connectionManager.getOrConnect(address);
        return sendResponseFailure(packet);
    }

    protected void throwCME(final Object key) {
        throw new ConcurrentModificationException("Another thread holds a lock for the key : "
                + key);
    }

    void enqueueEvent(int eventType, String name, Data key, Data value, Address from, boolean localEvent) {
        try {
            Member member = getMember(from);
            if (member == null) {
                member = new MemberImpl(from, thisAddress.equals(from));
            }
            Data newValue = value;
            Data oldValue = null;
            if (value != null && getInstanceType(name).isMap()) {
                Keys keys = (Keys) toObject(value);
                Collection<Data> values = keys.getKeys();
                if (values != null) {
                    Iterator<Data> it = values.iterator();
                    if (it.hasNext()) {
                        newValue = it.next();
                    }
                    if (it.hasNext()) {
                        oldValue = it.next();
                    }
                }
            }
            final DataAwareEntryEvent dataAwareEntryEvent = new DataAwareEntryEvent(member, eventType, name, key, newValue, oldValue, localEvent);
            int hash;
            if (key != null) {
                hash = key.hashCode();
            } else {
                hash = hashTwo(from.hashCode(), name.hashCode());
            }
            node.executorManager.getEventExecutorService().executeOrderedRunnable(hash, new Runnable() {
                public void run() {
                    try {
                        node.listenerManager.callListeners(dataAwareEntryEvent);
                    } catch (Exception e) {
                        logger.log(Level.WARNING, e.getMessage(), e);
                    }
                }
            });
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    public final void checkServiceThread() {
        if (Thread.currentThread() != node.serviceThread) {
            String msg = "Only ServiceThread can access this method. " + Thread.currentThread();
            logger.log(Level.SEVERE, msg);
            throw new Error(msg);
        }
    }

    static int hashTwo(int hash1, int hash2) {
        return hash1 * 29 + hash2;
    }

    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Data value, Address callerAddress) {
        fireMapEvent(mapListeners, name, eventType, null, value, callerAddress);
    }

    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Data oldValue, final Data value, Address callerAddress) {
        fireMapEvent(mapListeners, name, eventType, null, oldValue, value, null, callerAddress);
    }

    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Data key, final Data oldValue, final Data value,
                      Map<Address, Boolean> keyListeners, Address callerAddress) {
        if (keyListeners == null && (mapListeners == null || mapListeners.size() == 0)) {
            return;
        }
        try {
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
            Data packetValue = value;
            if (value != null && getInstanceType(name).isMap()) {
                Keys keys = new Keys();
                keys.add(value);
                if (oldValue != null) {
                    keys.add(oldValue);
                }
                packetValue = toData(keys);
            }
            sendEvents(eventType, name, key, packetValue, mapTargetListeners, callerAddress);
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    MemberImpl getMember(Address address) {
        return node.clusterManager.getMember(address);
    }

    void registerListener(boolean add, String name, Data key, Address address, boolean includeValue) {
        if (name.startsWith(Prefix.QUEUE)) {
            node.blockingQueueManager.registerListener(add, name, key, address, includeValue);
        } else if (name.startsWith(Prefix.TOPIC)) {
            node.topicManager.registerListener(add, name, key, address, includeValue);
        } else {
            node.concurrentMapManager.registerListener(add, name, key, address, includeValue);
        }
    }

    public final void handleResponse(Packet packetResponse) {
        final Call call = getRemoteCall(packetResponse.callId);
        if (call != null) {
            call.handleResponse(packetResponse);
        } else {
            logger.log(Level.FINEST, packetResponse.operation + " No call for callId " + packetResponse.callId);
            releasePacket(packetResponse);
        }
    }

    protected boolean send(Packet packet, Address address) {
        if (address == null) return false;
        final Connection conn = node.connectionManager.getOrConnect(address);
        return conn != null && conn.live() && writePacket(conn, packet);
    }

    protected final boolean send(Packet packet, Connection conn) {
        return conn != null && conn.live() && writePacket(conn, packet);
    }

    protected final boolean sendOrReleasePacket(Packet packet, Connection conn) {
        if (conn != null && conn.live() && writePacket(conn, packet)) {
            return true;
        }
        releasePacket(packet);
        return false;
    }

    private boolean writePacket(Connection conn, Packet packet) {
        final MemberImpl memberImpl = getMember(conn.getEndPoint());
        if (memberImpl != null) {
            memberImpl.didWrite();
        }
        if (packet.lockAddress != null) {
            if (thisAddress.equals(packet.lockAddress)) {
                packet.lockAddress = null;
            }
        }
        conn.getWriteHandler().enqueueSocketWritable(packet);
        return true;
    }
}
