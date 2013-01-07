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

package com.hazelcast.collection.processor;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @ali 1/2/13
 */
public abstract class BaseEntryProcessor<T> implements EntryProcessor<T> {

    private boolean binary = true;

    protected BaseEntryProcessor(){
    }

    protected BaseEntryProcessor(boolean binary) {
        this.binary = binary;
    }

    public boolean isBinary() {
        return binary;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(binary);
    }

    public void readData(ObjectDataInput in) throws IOException {
        binary = in.readBoolean();
    }
}
