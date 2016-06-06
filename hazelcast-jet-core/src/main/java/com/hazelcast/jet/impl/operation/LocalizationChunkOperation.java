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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class LocalizationChunkOperation extends JetOperation {
    private Chunk chunk;

    @SuppressWarnings("unused")
    public LocalizationChunkOperation() {

    }

    public LocalizationChunkOperation(String name,
                                      Chunk chunk) {
        super(name);
        this.chunk = chunk;
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = getApplicationContext();
        applicationContext.getLocalizationStorage().receiveFileChunk(this.chunk);
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(chunk);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        chunk = in.readObject();
    }
}
