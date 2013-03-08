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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;

public class TransactionKey {
    private String txnId;
    private String name;
    private Data key;

    public TransactionKey(String txnId, String name, Data key) {
        this.txnId = txnId;
        this.name = name;
        this.key = key;
    }

    public String getTxnId() {
        return txnId;
    }

    public String getName() {
        return name;
    }

    public Data getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionKey that = (TransactionKey) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (!name.equals(that.name)) return false;
        if (!txnId.equals(that.txnId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = txnId.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionKey");
        sb.append("{txnId='").append(txnId).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", key=").append(key);
        sb.append('}');
        return sb.toString();
    }
}
