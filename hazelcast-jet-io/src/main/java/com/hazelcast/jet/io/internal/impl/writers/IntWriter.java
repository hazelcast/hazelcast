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

package com.hazelcast.jet.io.internal.impl.writers;

import com.hazelcast.jet.io.api.ObjectWriter;
import com.hazelcast.jet.io.api.ObjectWriterFactory;
import com.hazelcast.jet.io.api.Types;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class IntWriter implements ObjectWriter<Integer> {
    @Override
    public void writeType(Integer object,
                          ObjectDataOutput objectDataOutput,
                          ObjectWriterFactory objectWriterFactory) throws IOException {
        objectDataOutput.writeByte(Types.INT.getTypeID());
    }

    @Override
    public void writePayLoad(Integer object,
                             ObjectDataOutput objectDataOutput,
                             ObjectWriterFactory objectWriterFactory) throws IOException {
        objectDataOutput.writeInt(object);
    }

    @Override
    public void write(Integer object,
                      ObjectDataOutput objectDataOutput,
                      ObjectWriterFactory objectWriterFactory) throws IOException {
        writeType(object, objectDataOutput, objectWriterFactory);
        writePayLoad(object, objectDataOutput, objectWriterFactory);
    }
}
