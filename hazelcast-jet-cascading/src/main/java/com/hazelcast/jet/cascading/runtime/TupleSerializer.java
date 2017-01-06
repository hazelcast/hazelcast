/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.runtime;

import cascading.tuple.Tuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class TupleSerializer implements StreamSerializer<Tuple> {
    @Override
    public void write(ObjectDataOutput objectDataOutput, Tuple tuple) throws IOException {
        int size = tuple.size();
        objectDataOutput.writeInt(size);
        for (int i = 0; i < size; i++) {
            objectDataOutput.writeObject(tuple.getObject(i));
        }
    }

    @Override
    public Tuple read(ObjectDataInput objectDataInput) throws IOException {
        int size = objectDataInput.readInt();
        Tuple tuple = Tuple.size(size);
        for (int i = 0; i < size; i++) {
            tuple.set(i, objectDataInput.readObject());
        }
        return tuple;
    }

    @Override
    public int getTypeId() {
        return 1;
    }

    @Override
    public void destroy() {

    }
}
