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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.KeyPredicate;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractMapReduceOperationFactory<KeyIn, ValueIn, KeyOut, ValueOut>
        implements OperationFactory {

    protected KeyValueSource<KeyIn, ValueIn> keyValueSource;

    protected Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    protected CombinerFactory<KeyOut, ValueOut, ?> combinerFactory;

    protected KeyPredicate<KeyIn> predicate;

    protected List<KeyIn> keys;

    protected int chunkSize;

    protected String name;

    protected AbstractMapReduceOperationFactory() {
    }

    protected AbstractMapReduceOperationFactory(String name, int chunkSize, List<KeyIn> keys,
                                                KeyPredicate<KeyIn> predicate,
                                                KeyValueSource<KeyIn, ValueIn> keyValueSource,
                                                Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                                CombinerFactory<KeyOut, ValueOut, ?> combinerFactory) {
        this.name = name;
        this.chunkSize = chunkSize;
        this.keys = keys;
        this.predicate = predicate;
        this.keyValueSource = keyValueSource;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(mapper);
        out.writeObject(keyValueSource);
        out.writeObject(combinerFactory);
        out.writeInt(chunkSize);
        out.writeInt(keys == null ? 0 : keys.size());
        if (keys != null) {
            for (KeyIn key : keys) {
                out.writeObject(key);
            }
        }
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        mapper = in.readObject();
        keyValueSource = in.readObject();
        combinerFactory = in.readObject();
        chunkSize = in.readInt();
        int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<KeyIn>(size);
            for (int i = 0; i < size; i++) {
                keys.add((KeyIn) in.readObject());
            }
        }
        predicate = in.readObject();
    }

}
