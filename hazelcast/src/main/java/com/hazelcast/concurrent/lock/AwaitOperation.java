/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

public class AwaitOperation extends BaseConditionOperation implements WaitSupport {

    public AwaitOperation() {
    }

    public AwaitOperation(ILockNamespace namespace, Data key, int threadId) {
        super(namespace, key, threadId);
    }

    public void run() throws Exception {
        getLockStore().lock(key, getCallerUuid(), threadId);
    }

    public boolean returnsResponse() {
        return isLockOwner;
    }

    public final long getWaitTimeoutMillis() {
        return timeout;
    }

    public final WaitNotifyKey getWaitKey() {
        return new ConditionWaitNotifyKey(key);
    }

    public final boolean shouldWait() {
        return isLockOwner;
    }

    public final void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

}
