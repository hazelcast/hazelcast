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
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;

public final class LockWaitNotifyKey implements WaitNotifyKey {

    private final ObjectNamespace namespace;
    private final Data key;

    public LockWaitNotifyKey(ObjectNamespace namespace, Data key) {
        this.namespace = namespace;
        this.key = key;
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

        LockWaitNotifyKey that = (LockWaitNotifyKey) o;

        if (!key.equals(that.key)) {
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
        result = 31 * result + key.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "LockWaitNotifyKey{"
                + "namespace=" + namespace
                + ", key=" + key
                + '}';
    }
}
