/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.CancellationException;

public class ClientCancellationRequest
        extends InvocationClientRequest
        implements IdentifiedDataSerializable {

    private String name;
    private String jobId;

    public ClientCancellationRequest() {
    }

    public ClientCancellationRequest(String name, String jobId) {
        this.name = name;
        this.jobId = jobId;
    }


    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    protected void invoke() {
        final ClientEndpoint endpoint = getEndpoint();
        final ClientEngine engine = getClientEngine();

        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(name, jobId);

        if (supervisor == null || !supervisor.isOwnerNode()) {
            engine.sendResponse(endpoint, Boolean.FALSE);
        }
        Exception exception = new CancellationException("Operation was cancelled by the user");
        engine.sendResponse(endpoint, supervisor.cancelAndNotify(exception));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(jobId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        jobId = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.CLIENT_CANCELLATION_REQUEST;
    }

}
