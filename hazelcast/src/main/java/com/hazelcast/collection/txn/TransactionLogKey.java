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

package com.hazelcast.collection.txn;

class TransactionLogKey {

    private final String name;

    private final long itemId;

    private final String serviceName;

    TransactionLogKey(String name, long itemId, String serviceName) {
        this.name = name;
        this.itemId = itemId;
        this.serviceName = serviceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionLogKey)) {
            return false;
        }

        TransactionLogKey that = (TransactionLogKey) o;

        if (itemId != that.itemId) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!serviceName.equals(that.serviceName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (itemId ^ (itemId >>> 32));
        result = 31 * result + serviceName.hashCode();
        return result;
    }
}
