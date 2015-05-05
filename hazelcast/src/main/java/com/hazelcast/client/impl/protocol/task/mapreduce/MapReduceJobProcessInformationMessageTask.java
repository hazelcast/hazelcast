/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.mapreduce;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.MapReduceJobProcessInformationParameters;
import com.hazelcast.client.impl.protocol.parameters.MapReduceJobProcessInformationResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.mapreduce.JobProcessInformation;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.nio.Connection;

import java.security.Permission;

public class MapReduceJobProcessInformationMessageTask
        extends AbstractCallableMessageTask<MapReduceJobProcessInformationParameters> {

    public MapReduceJobProcessInformationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        MapReduceService mapReduceService = getService(MapReduceService.SERVICE_NAME);
        JobSupervisor supervisor = mapReduceService.getJobSupervisor(parameters.name, parameters.jobId);

        if (supervisor != null && supervisor.getJobProcessInformation() != null) {
            JobProcessInformation current = supervisor.getJobProcessInformation();
            return MapReduceJobProcessInformationResultParameters.encode(current.getPartitionStates(),
                    current.getProcessedRecords());
        }
        throw new IllegalStateException("Information not found for map reduce with name : "
                + parameters.name + ", job id : " + parameters.jobId);
    }

    @Override
    protected MapReduceJobProcessInformationParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapReduceJobProcessInformationParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
