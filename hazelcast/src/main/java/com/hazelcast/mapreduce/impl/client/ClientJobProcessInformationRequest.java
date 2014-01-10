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
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.TransferableJobProcessInformation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class ClientJobProcessInformationRequest
        extends InvocationClientRequest
        implements IdentifiedDataSerializable {

    private String name;
    private String jobId;

    public ClientJobProcessInformationRequest() {
    }

    public ClientJobProcessInformationRequest(String name, String jobId) {
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

        JobProcessInformation processInformation = null;
        if (supervisor != null && supervisor.getJobProcessInformation() != null) {
            JobProcessInformation current = supervisor.getJobProcessInformation();
            processInformation = new TransferableJobProcessInformation(
                    current.getPartitionStates(), current.getProcessedRecords());
        }
        engine.sendResponse(endpoint, processInformation);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.CLIENT_JOB_PROCESS_INFO_REQUEST;
    }

}
