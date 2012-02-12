/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.concurrentmap;

import com.hazelcast.nio.Data;

import static com.hazelcast.nio.IOUtil.toObject;

public class ValueHolder {
    final Data data;
    volatile Object value;

    public ValueHolder(Data data) {
        this.data = data;
    }

    public Data getData() {
        return data;
    }

    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    public Object getValue() {
        Object v = value;
        if (v != null) return v;
        //noinspection SynchronizeOnThis
        synchronized (ValueHolder.this) {
            value = toObject(data);
            return value;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueHolder that = (ValueHolder) o;
        Object v = getValue();
        return v.equals(that.getValue());
    }

    @Override
    public int hashCode() {
        final Object v = getValue();
        return v != null ? v.hashCode() : Integer.MIN_VALUE;
    }
}
