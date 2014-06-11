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

package com.hazelcast.management.request;

import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedHashMap;
import java.util.Map;

// author: sancar - 11.12.2012
public class ClusterPropsRequest implements ConsoleRequest {

    public ClusterPropsRequest() {

    }

    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_PROPERTIES;
    }

    public Object readResponse(ObjectDataInput in) throws IOException {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        int size = in.readInt();
        String[] temp;
        for (int i = 0; i < size; i++) {
            temp = in.readUTF().split(":#");
            properties.put(temp[0], temp.length == 1 ? "" : temp[1]);
        }
        return properties;
    }

    public void writeResponse(ManagementCenterService mcs, ObjectDataOutput dos) throws Exception {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        Runtime runtime = Runtime.getRuntime();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        InternalPartitionServiceImpl partitionService = mcs.getHazelcastInstance().node.getPartitionService();
        properties.put("hazelcast.cl_version", mcs.getHazelcastInstance().node.initializer.getVersion());
        properties.put("date.cl_startTime", Long.toString(runtimeMxBean.getStartTime()));
        properties.put("seconds.cl_upTime", Long.toString(runtimeMxBean.getUptime()));
        properties.put("memory.cl_freeMemory", Long.toString(runtime.freeMemory()));
        properties.put("memory.cl_totalMemory", Long.toString(runtime.totalMemory()));
        properties.put("memory.cl_maxMemory", Long.toString(runtime.maxMemory()));
        properties.put("return.hasOngoingMigration" , Boolean.toString(partitionService.hasOnGoingMigration()));
        properties.put("data.cl_migrationTasksCount", Long.toString(partitionService.getMigrationQueueSize()));

        dos.writeInt(properties.size());

        for (Object property : properties.keySet()) {
            dos.writeUTF((String) property + ":#" + (String) properties.get(property));
        }

    }

    public void writeData(ObjectDataOutput out) throws IOException {

    }

    public void readData(ObjectDataInput in) throws IOException {

    }
}
