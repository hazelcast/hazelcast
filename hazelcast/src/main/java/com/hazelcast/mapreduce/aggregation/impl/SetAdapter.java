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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A simple adapter class to serialize values of a {@link java.util.Set} using
 * Hazelcast serialization support.
 *
 * @param <E> element type of the set
 */
public class SetAdapter<E>
        extends HashSet<E>
        implements IdentifiedDataSerializable {

    @Override
    public int getFactoryId() {
        return AggregationsDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregationsDataSerializerHook.SET_ADAPTER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        out.writeInt(size());
        for (E element : this) {
            out.writeObject(element);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        int size = in.readInt();
        Set<E> set = new HashSet<E>(size);
        for (int i = 0; i < size; i++) {
            set.add((E) in.readObject());
        }
        addAll(set);
    }
}
