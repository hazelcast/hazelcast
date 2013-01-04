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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.processor.BaseEntryProcessor;
import com.hazelcast.config.MultiMapConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ali 1/3/13
 */
public abstract class MultiMapEntryProcessor<T> extends BaseEntryProcessor<T> {

    String collectionType = MultiMapConfig.ValueCollectionType.SET.toString();

    protected MultiMapEntryProcessor() {
    }

    protected MultiMapEntryProcessor(boolean binary, MultiMapConfig.ValueCollectionType collectionType) {
        super(binary);
        this.collectionType = collectionType.toString();
    }

    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(collectionType);
    }

    public void readData(DataInput in) throws IOException {
        super.readData(in);
        collectionType = in.readUTF();
    }
}
