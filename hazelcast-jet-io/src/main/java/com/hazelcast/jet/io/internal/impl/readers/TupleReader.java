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

package com.hazelcast.jet.io.internal.impl.readers;

import com.hazelcast.jet.io.api.ObjectReader;
import com.hazelcast.jet.io.api.ObjectReaderFactory;
import com.hazelcast.jet.io.api.tuple.TupleFactory;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;

public class TupleReader implements ObjectReader<Tuple> {
    private final TupleFactory defaultTupleFactory;

    public TupleReader(TupleFactory defaultTupleFactory) {
        this.defaultTupleFactory = defaultTupleFactory;
    }

    @Override
    public Tuple read(ObjectDataInput objectDataInput,
                      ObjectReaderFactory objectReaderFactory) throws IOException {
        int keySize = objectDataInput.readInt();
        int valueSize = objectDataInput.readInt();

        Object key = readTupleEntries(objectDataInput, objectReaderFactory, keySize);
        Object value = readTupleEntries(objectDataInput, objectReaderFactory, valueSize);

        if (keySize > 1) {
            return
                    valueSize > 1
                            ?
                            defaultTupleFactory.tuple((Object[]) key, (Object[]) value)
                            :
                            defaultTupleFactory.tuple((Object[]) key, value);
        } else {
            return
                    valueSize > 1
                            ?
                            defaultTupleFactory.tuple(key, (Object[]) value)
                            :
                            defaultTupleFactory.tuple(key, value);

        }
    }

    private Object readTupleEntries(ObjectDataInput objectDataInput,
                                    ObjectReaderFactory objectReaderFactory, int size) throws IOException {
        if (size == 1) {
            byte typeId = objectDataInput.readByte();
            return objectReaderFactory.getReader(typeId).read(objectDataInput, objectReaderFactory);
        } else {
            Object[] entries = new Object[size];

            for (int i = 0; i < size; i++) {
                byte typeId = objectDataInput.readByte();
                entries[i] = objectReaderFactory.getReader(typeId).read(objectDataInput, objectReaderFactory);
            }

            return entries;
        }
    }
}
