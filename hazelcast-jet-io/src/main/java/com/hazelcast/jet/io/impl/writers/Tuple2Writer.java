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

package com.hazelcast.jet.io.impl.writers;

import com.hazelcast.jet.io.ObjectWriter;
import com.hazelcast.jet.io.ObjectWriterFactory;
import com.hazelcast.jet.io.Types;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class Tuple2Writer implements ObjectWriter<Tuple2> {
    @Override
    public void writeType(Tuple2 object, ObjectDataOutput objectDataOutput, ObjectWriterFactory objectWriterFactory)
    throws IOException {
        objectDataOutput.writeByte(Types.TUPLE2.getTypeID());
    }

    @Override
    public void writePayload(
            Tuple2 tuple, ObjectDataOutput objectDataOutput, ObjectWriterFactory objectWriterFactory)
    throws IOException {
        for (int i = 0; i < 2; i++) {
            objectWriterFactory.getWriter(tuple.get(i)).write(tuple.get(i), objectDataOutput, objectWriterFactory);
        }
    }

    @Override
    public void write(Tuple2 tuple, ObjectDataOutput objectDataOutput, ObjectWriterFactory objectWriterFactory)
    throws IOException {
        writeType(tuple, objectDataOutput, objectWriterFactory);
        writePayload(tuple, objectDataOutput, objectWriterFactory);
    }
}
