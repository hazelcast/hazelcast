/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.durableexecutor.impl.operations;

import com.hazelcast.durableexecutor.impl.DistributedDurableExecutorService;
import com.hazelcast.spi.impl.operationservice.AbstractWaitNotifyKey;

public class DurableExecutorWaitNotifyKey extends AbstractWaitNotifyKey {

    private final long uniqueId;

    DurableExecutorWaitNotifyKey(String objectName, long uniqueId) {
        super(DistributedDurableExecutorService.SERVICE_NAME, objectName);
        this.uniqueId = uniqueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DurableExecutorWaitNotifyKey)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        DurableExecutorWaitNotifyKey that = (DurableExecutorWaitNotifyKey) o;
        return (uniqueId == that.uniqueId);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (uniqueId ^ (uniqueId >>> 32));
        return result;
    }
}
