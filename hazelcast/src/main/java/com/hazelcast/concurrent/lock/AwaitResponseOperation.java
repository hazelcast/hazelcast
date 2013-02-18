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
//import com.hazelcast.spi.WaitSupport;
//
//import java.io.IOException;
//
//public class AwaitResponseOperation extends BaseLockOperation implements WaitSupport {
//
//    private String conditionId;
//    private transient boolean expired = false;
//
//    public AwaitResponseOperation() {
//    }
//
//    public AwaitResponseOperation(ILockNamespace namespace, Data key, int threadId, long timeout, String conditionId) {
//        super(namespace, key, threadId, timeout);
//        this.conditionId = conditionId;
//    }
//
//    public void run() throws Exception {
//        if (!getLockStore().lock(key, getCallerUuid(), threadId)) {
//            throw new IllegalMonitorStateException("Current thread is not owner of the lock!");
//        }
//        LockService service = getService();
//        if (!expired) {
////            service.removeSignalKey(getWaitKey());
////            service.removeAwait(getWaitKey());
//            response = true;
//        } else {
//            response = false;
//        }
//    }
//
//    public ConditionKey getWaitKey() {
//        return new ConditionKey(key, conditionId);
//    }
//
//    public boolean shouldWait() {
//        return !getLockStore().canAcquireLock(key, getCallerUuid(), threadId);
//    }
//
//    public long getWaitTimeoutMillis() {
//        return timeout;
//    }
//
//    public void onWaitExpire() {
//        expired = true;
//        LockService service = getService();
//        final ConditionKey waitKey = getWaitKey();
////        service.removeSignalKey(waitKey);
////        service.removeAwait(waitKey);
//
//        if (getLockStore().lock(key, getCallerUuid(), threadId)) {
//            getResponseHandler().sendResponse(false); // expired & acquired lock, send FALSE
//        } else {
//            // expired but could not acquire lock, no response atm
////            service.registerExpiredAwaitOp(this);
//        }
//    }
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
