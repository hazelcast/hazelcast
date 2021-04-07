/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GetJobIdsByNameOperation
        extends AsyncOperation
        implements IdentifiedDataSerializable, AllowedDuringPassiveState {

    private String name;

    public GetJobIdsByNameOperation() {
    }

    public GetJobIdsByNameOperation(String name) {
        this.name = name;
    }

    @Override
    public CompletableFuture<List<Long>> doRun() {
        return getJobCoordinationService().getJobIds(name);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.GET_JOB_IDS_BY_NAME_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
    }
}
