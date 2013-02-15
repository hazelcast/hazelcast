///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.concurrent.lock;
//
//import com.hazelcast.nio.ObjectDataInput;
//import com.hazelcast.nio.ObjectDataOutput;
//import com.hazelcast.nio.serialization.Data;
//import com.hazelcast.spi.Notifier;
//import com.hazelcast.spi.OperationAccessor;
//import com.hazelcast.spi.WaitNotifyKey;
//
//import java.io.IOException;
//
//public class AwaitRequestOperation extends BaseLockOperation implements Notifier {
//
//    private String conditionId;
//    private transient boolean isLockOwner;
//
//    public AwaitRequestOperation() {
//    }
//
//    public AwaitRequestOperation(ILockNamespace namespace, Data key, int threadId, long timeout, String conditionId) {
//        super(namespace, key, threadId, timeout);
//        this.conditionId = conditionId;
//    }
//
//    public void beforeRun() throws Exception {
//        final LockStore lockStore = getLockStore();
//        isLockOwner = lockStore.isLocked(key) && lockStore.canAcquireLock(key, getCallerUuid(), threadId);
//        if (!isLockOwner) {
//            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
//        }
//    }
//
//    public void run() throws Exception {
//        LockService service = getService();
////        service.addAwait(new ConditionKey(key, conditionId));
//
//        getLockStore().unlock(key, getCallerUuid(), threadId);
//
//        final AwaitResponseOperation op = new AwaitResponseOperation(namespace, key, threadId, timeout, conditionId);
//        op.setNodeEngine(getNodeEngine()).setService(getService()).setResponseHandler(getResponseHandler())
//                .setPartitionId(getPartitionId()).setReplicaIndex(getReplicaIndex()).setCallerUuid(getCallerUuid());
//        OperationAccessor.setCallId(op, getCallId());
//        OperationAccessor.setInvocationTime(op, getInvocationTime());
//        OperationAccessor.setCallTimeout(op, getCallTimeout());
//        getNodeEngine().getWaitNotifyService().await(op);
//    }
//
//    public boolean returnsResponse() {
//        return !isLockOwner;
//    }
//
//    public boolean shouldNotify() {
//        return true;
//    }
//
//    public WaitNotifyKey getNotifiedKey() {
//        return new LockWaitNotifyKey(namespace, key);
//    }
//
////    @Override
////    public InvocationAction onException(Throwable throwable) {
////        if (throwable instanceof MemberLeftException) {
////            return InvocationAction.THROW_EXCEPTION;
////        }
////        return super.onException(throwable);
////    }
//
//    @Override
//    protected void writeInternal(ObjectDataOutput out) throws IOException {
//        super.writeInternal(out);
//        out.writeUTF(conditionId);
//    }
//
//    @Override
//    protected void readInternal(ObjectDataInput in) throws IOException {
//        super.readInternal(in);
//        conditionId = in.readUTF();
//    }
//}
