/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnqueue;

class TransactionLogRecordKey {

    private final long itemId;

    private final String name;

    public TransactionLogRecordKey(long itemId, String name) {
        this.itemId = itemId;
        this.name = name;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionLogRecordKey)) {
            return false;
        }

        TransactionLogRecordKey that = (TransactionLogRecordKey) o;

        if (itemId != that.itemId) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }

        return true;
    }

    public int hashCode() {
        int result = (int) (itemId ^ (itemId >>> 32));
        result = 31 * result + name.hashCode();
        return result;
    }
}
