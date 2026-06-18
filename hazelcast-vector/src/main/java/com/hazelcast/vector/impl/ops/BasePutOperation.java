/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.vector.VectorValues;

import java.io.IOException;

/**
 * Common logic for all 1-entry update operations
 */
public abstract class BasePutOperation extends BaseMutatingOperation {
    protected Data key;
    protected Data userValue;
    protected VectorValues vectorValues;

    protected BasePutOperation() {
    }

    protected BasePutOperation(String vectorCollectionName, Data key, Data userValue, VectorValues vectorValues) {
        super(vectorCollectionName);
        this.key = key;
        this.userValue = userValue;
        this.vectorValues = vectorValues;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, userValue);
        out.writeObject(vectorValues);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.key = IOUtil.readData(in);
        this.userValue = IOUtil.readData(in);
        this.vectorValues = in.readObject();
    }
}
