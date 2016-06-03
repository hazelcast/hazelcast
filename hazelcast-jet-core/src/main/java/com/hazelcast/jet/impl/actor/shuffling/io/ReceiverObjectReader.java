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

package com.hazelcast.jet.impl.actor.shuffling.io;

import com.hazelcast.jet.io.ObjectReader;
import com.hazelcast.jet.io.ObjectReaderFactory;
import com.hazelcast.nio.ObjectDataInput;

import java.io.IOException;

public class ReceiverObjectReader {
    private final ObjectDataInput objectDataInput;
    private final ObjectReaderFactory objectReaderFactory;

    public ReceiverObjectReader(ObjectDataInput objectDataInput,
                                ObjectReaderFactory objectReaderFactory) {
        this.objectDataInput = objectDataInput;
        this.objectReaderFactory = objectReaderFactory;
    }

    public int read(Object[] dataChunkBuffer, int dataChunkLength) throws IOException {
        int dataChunkIndex = 0;

        while (dataChunkIndex < dataChunkLength) {
            byte typeId = this.objectDataInput.readByte();
            ObjectReader objectReader = this.objectReaderFactory.getReader(typeId);
            dataChunkBuffer[dataChunkIndex++] = objectReader.read(
                    this.objectDataInput,
                    this.objectReaderFactory
            );
        }

        return dataChunkIndex;
    }
}
