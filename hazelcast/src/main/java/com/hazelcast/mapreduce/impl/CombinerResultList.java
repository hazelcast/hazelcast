/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This {@link java.util.ArrayList} subclass exists to prevent
 * {@link com.hazelcast.mapreduce.impl.task.DefaultContext.CollectingCombinerFactory}
 * created collections to be mixed up with user provided List results from
 * {@link com.hazelcast.mapreduce.Combiner}s.
 * @param <E> element type of the collection
 */
public class CombinerResultList<E> extends ArrayList<E>
        implements IdentifiedDataSerializable {

    public CombinerResultList() {
        super();
    }

    public CombinerResultList(Collection<? extends E> c) {
        super(c);
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.COMBINER_RESULT_LIST;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        int size = size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeObject(get(i));
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            add(i, (E) in.readObject());
        }
    }
}
