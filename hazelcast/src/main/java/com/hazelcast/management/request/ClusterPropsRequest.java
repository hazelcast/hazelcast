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

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterPropsRequest implements ConsoleRequest {

    public ClusterPropsRequest() {
    }

    @Override
    public int getType() {
        return ConsoleRequestConstants.REQUEST_TYPE_CLUSTER_PROPERTIES;
    }

    @Override
    public Object readResponse(JsonObject in) {
        Map<String, String> properties = new LinkedHashMap<String, String>();
        final Iterator<JsonObject.Member> iterator = in.iterator();
        while (iterator.hasNext()) {
            final JsonObject.Member property = iterator.next();
            properties.put(property.getName(), property.getValue().asString());
        }
        return properties;
    }

    @Override
    public void writeResponse(ManagementCenterService mcs, JsonObject root) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        InternalPartitionService partitionService = mcs.getHazelcastInstance().node.getPartitionService();

        JsonObject properties = new JsonObject();
        properties.add("hazelcast.cl_version", mcs.getHazelcastInstance().node.getBuildInfo().getVersion());
        properties.add("date.cl_startTime", Long.toString(runtimeMxBean.getStartTime()));
        properties.add("seconds.cl_upTime", Long.toString(runtimeMxBean.getUptime()));
        properties.add("memory.cl_freeMemory", Long.toString(runtime.freeMemory()));
        properties.add("memory.cl_totalMemory", Long.toString(runtime.totalMemory()));
        properties.add("memory.cl_maxMemory", Long.toString(runtime.maxMemory()));
        properties.add("return.hasOngoingMigration", Boolean.toString(partitionService.hasOnGoingMigration()));
        properties.add("data.cl_migrationTasksCount", Long.toString(partitionService.getMigrationQueueSize()));
        root.add("result", properties);
    }

    @Override
    public JsonValue toJson() {
        return new JsonObject();
    }

    @Override
    public void fromJson(JsonObject json) {

    }
}
