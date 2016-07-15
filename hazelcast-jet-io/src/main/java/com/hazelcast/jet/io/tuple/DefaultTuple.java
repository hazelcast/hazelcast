/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.io.tuple;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTuple implements Tuple {
    protected Object[] store;

    public DefaultTuple() {
    }

    public DefaultTuple(Object c0, Object c1) {
        checkNotNull(c0);
        checkNotNull(c1);
        this.store = new Object[2];
        store[0] = c0;
        store[1] = c1;
    }

    public DefaultTuple(Object[] components) {
        checkNotNull(components);
        this.store = components.clone();
    }

    @Override
    public Object get(int index) {
        return store[index];
    }

    @Override
    public int size() {
        return store.length;
    }

    @Override
    public void set(int index, Object value) {
        store[index] = value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] toArray() {
        return store.clone();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(store);
        out.writeInt(store.length);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        store = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || o != null && o.getClass() == DefaultTuple.class && Arrays.equals(store, ((DefaultTuple) o).store);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(store);
    }
}
