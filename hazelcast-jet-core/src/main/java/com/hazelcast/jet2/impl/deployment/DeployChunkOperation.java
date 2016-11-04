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

package com.hazelcast.jet2.impl.deployment;

import com.hazelcast.jet2.impl.EngineOperation;
import com.hazelcast.jet2.impl.JetService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class DeployChunkOperation extends EngineOperation {
    private ResourceChunk chunk;

    @SuppressWarnings("unused")
    public DeployChunkOperation() {
    }

    public DeployChunkOperation(String engineName, ResourceChunk chunk) {
        super(engineName);
        this.chunk = chunk;
    }

    @Override
    public void run() throws Exception {
        JetService service = getService();
        service.getEngineContext(engineName).getDeploymentStore().receiveChunk(chunk);
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeObject(chunk);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        chunk = in.readObject();
    }
}
