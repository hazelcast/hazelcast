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

import com.hazelcast.mapreduce.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractMapReduceOperation<KeyIn, ValueIn, KeyOut, ValueOut>
        extends AbstractNamedOperation
        implements PartitionAwareOperation {

    protected Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper;

    protected CombinerFactory<KeyOut, ValueOut, ?> combinerFactory;

    protected KeyPredicate<KeyIn> predicate;

    protected List<KeyIn> keys;

    protected int chunkSize;

    protected transient Object response;

    protected AbstractMapReduceOperation() {
    }

    protected AbstractMapReduceOperation(String name, int chunkSize, List<KeyIn> keys,
                                         KeyPredicate<KeyIn> predicate,
                                         Mapper<KeyIn, ValueIn, KeyOut, ValueOut> mapper,
                                         CombinerFactory<KeyOut, ValueOut, ?> combinerFactory) {
        super(name);
        this.keys = keys;
        this.predicate = predicate;
        this.mapper = mapper;
        this.combinerFactory = combinerFactory;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public final void run() throws Exception {
        DefaultContext<KeyOut, ValueOut> context = createContext();

        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).initialize(context);
        }
        mappingPhase(context);
        if (mapper instanceof LifecycleMapper) {
            ((LifecycleMapper) mapper).finalized(context);
        }

        Map<KeyOut, Object> chunkMap = context.finish();
        // TODO Send final chunk from this partition
        // Wrap into LastChunkResponse object
        response = chunkMap;
    }

    protected abstract void mappingPhase(Context<KeyOut, ValueOut> context);

    protected boolean matches(KeyIn key) {
        if ((keys == null || keys.size() == 0) && predicate == null) {
            return true;
        }
        if (keys != null && keys.size() > 0) {
            for (KeyIn matcher : keys) {
                if (key.equals(matcher)) {
                    return true;
                }
            }
        }
        if (predicate != null) {
            return predicate.evaluate(key);
        }
        return false;
    }

    protected DefaultContext<KeyOut, ValueOut> createContext() {
        return new DefaultContext<KeyOut, ValueOut>(combinerFactory, this);
    }

    <Chunk> void onEmit(DefaultContext<KeyOut, ValueOut> context) {
        if (context.getCollected() == chunkSize) {
            Map<KeyOut, Chunk> chunkMap = context.requestChunk();
            // TODO Send chunkMap to reducers
            // Wrap into IntermediateChunkResponse object
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapper);
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
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapper = in.readObject();
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
