/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.util.Collection;

public final class LockWaitNotifyKeySet implements WaitNotifyKey {

    private final ObjectNamespace namespace;
    private final Collection<Data> keys;

    public LockWaitNotifyKeySet(ObjectNamespace namespace, Collection<Data> keys) {
        this.namespace = namespace;
        this.keys = keys;
    }

    @Override
    public String getServiceName() {
        return namespace.getServiceName();
    }

    @Override
    public String getObjectName() {
        return namespace.getObjectName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LockWaitNotifyKeySet that = (LockWaitNotifyKeySet) o;

        if (!keys.equals(that.keys)) {
            return false;
        }
        if (!namespace.equals(that.namespace)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = namespace.hashCode();
        result = 31 * result + keys.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LockWaitNotifyKeySet{"
                + "namespace=" + namespace
                + ", keySet=" + keys
                + '}';
    }
}
