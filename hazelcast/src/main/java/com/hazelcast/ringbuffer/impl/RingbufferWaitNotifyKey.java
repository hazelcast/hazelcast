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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.AbstractWaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A {@link AbstractWaitNotifyKey} to make it possible to wait
 * for an item to be published in the ringbuffer.
 * The exact ringbuffer is specified by the partition ID and namespace as those
 * two parameters uniquely identify a single ringbuffer inside the ringbuffer service.
 */
public class RingbufferWaitNotifyKey implements WaitNotifyKey {

    private final ObjectNamespace namespace;
    private final int partitionId;

    public RingbufferWaitNotifyKey(ObjectNamespace namespace, int partitionId) {
        checkNotNull(namespace);
        this.namespace = namespace;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RingbufferWaitNotifyKey that = (RingbufferWaitNotifyKey) o;

        return partitionId == that.partitionId && namespace.equals(that.namespace);
    }

    @Override
    public int hashCode() {
        int result = namespace.hashCode();
        result = 31 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        return "RingbufferWaitNotifyKey{"
                + "namespace=" + namespace
                + ", partitionId=" + partitionId
                + '}';
    }

    @Override
    public String getServiceName() {
        return namespace.getServiceName();
    }

    @Override
    public String getObjectName() {
        return namespace.getObjectName();
    }
}

