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
import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.impl.MapReducePortableHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.mapreduce.impl.task.TransferableJobProcessInformation;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.security.Permission;

/**
 * This class is used to retrieve current processed records and other statistics from
 * emitting client to the job owning node.
 */
public class ClientJobProcessInformationRequest extends InvocationClientRequest {

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

        MapReduceService mapReduceService = getService();
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(name, jobId);

        JobProcessInformation processInformation = null;
        if (supervisor != null && supervisor.getJobProcessInformation() != null) {
            JobProcessInformation current = supervisor.getJobProcessInformation();
            processInformation = new TransferableJobProcessInformation(current.getPartitionStates(),
                    current.getProcessedRecords());
        }
        endpoint.sendResponse(processInformation, getCallId());
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        writer.writeUTF("name", name);
        writer.writeUTF("jobId", jobId);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        name = reader.readUTF("name");
        jobId = reader.readUTF("jobId");
    }

    @Override
    public int getFactoryId() {
        return MapReducePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapReducePortableHook.CLIENT_JOB_PROCESS_INFO_REQUEST;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
