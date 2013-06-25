/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.nio.serialization.Data;

public class TransactionLogKey {

    final CollectionProxyId proxyId;

    final Data key;

    public TransactionLogKey(CollectionProxyId proxyId, Data key) {
        this.proxyId = proxyId;
        this.key = key;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionLogKey)) return false;

        TransactionLogKey that = (TransactionLogKey) o;

        if (!key.equals(that.key)) return false;
        if (!proxyId.equals(that.proxyId)) return false;

        return true;
    }

    public int hashCode() {
        int result = proxyId.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
