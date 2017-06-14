/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A {@link com.hazelcast.spi.AbstractWaitNotifyKey} to make it possible to wait for an item to be published in the ringbuffer.
 */
public class RingbufferWaitNotifyKey implements WaitNotifyKey {

    private final ObjectNamespace namespace;

    public RingbufferWaitNotifyKey(ObjectNamespace namespace) {
        checkNotNull(namespace);
        this.namespace = namespace;
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

        return namespace.equals(that.namespace);
    }

    @Override
    public int hashCode() {
        return namespace.hashCode();
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

