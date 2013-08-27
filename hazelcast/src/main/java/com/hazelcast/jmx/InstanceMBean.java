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

package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.ExecutionService;

import javax.management.*;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jmx.ManagementService.quote;

/**
 * @author ali 2/1/13
 */
@ManagedDescription("HazelcastInstance")
public class InstanceMBean extends HazelcastMBean<HazelcastInstanceImpl> {

    final Config config;
    final Cluster cluster;

    protected InstanceMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
        super(hazelcastInstance, managementService);

        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("type", quote("HazelcastInstance"));
        properties.put("name", quote(hazelcastInstance.getName()));
        setObjectName(properties);

        config = hazelcastInstance.getConfig();
        cluster = hazelcastInstance.getCluster();

        Node node = hazelcastInstance.node;
        ExecutionService executionService = node.nodeEngine.getExecutionService();

        register(new NodeMBean(hazelcastInstance,node,managementService));
        register(new ConnectionManagerMBean(hazelcastInstance,node.connectionManager,service));
        register(new EventServiceMBean(hazelcastInstance,node.nodeEngine.getEventService(),service));
        register(new OperationServiceMBean(hazelcastInstance,node.nodeEngine.getOperationService(),service));
        register(new ProxyServiceMBean(hazelcastInstance,node.nodeEngine.getProxyService(),service));
        register(new ClientEngineMBean(hazelcastInstance,node.clientEngine ,service));
        register(new ManagedExecutionServiceMBean(hazelcastInstance,executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR) ,service));
        register(new ManagedExecutionServiceMBean(hazelcastInstance,executionService.getExecutor(ExecutionService.OPERATION_EXECUTOR) ,service));
        register(new ManagedExecutionServiceMBean(hazelcastInstance,executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR) ,service));
        register(new ManagedExecutionServiceMBean(hazelcastInstance,executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR) ,service));
        register(new ManagedExecutionServiceMBean(hazelcastInstance,executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR) ,service));
        register(new ManagedExecutionServiceMBean(hazelcastInstance,executionService.getExecutor(ExecutionService.QUERY_EXECUTOR) ,service));
    }

    public HazelcastInstance getHazelcastInstance(){
        return managedObject;
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the Instance")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("String representation of config")
    public String getConfig() {
        return config.toString();
    }

    @ManagedAnnotation("configSource")
    @ManagedDescription("The source of config")
    public String getConfigSource() {
        File configurationFile = config.getConfigurationFile();
        if (configurationFile != null) {
            return configurationFile.getAbsolutePath();
        }
        URL configurationUrl = config.getConfigurationUrl();
        if (configurationUrl != null) {
            return configurationUrl.toString();
        }
        return null;
    }

    @ManagedAnnotation("groupName")
    @ManagedDescription("Group Name")
    public String getGroupName(){
        return config.getGroupConfig().getName();
    }

    @ManagedAnnotation("port")
    @ManagedDescription("Network Port")
    public int getPort(){
        return config.getNetworkConfig().getPort();
    }

    @ManagedAnnotation("clusterTime")
    @ManagedDescription("Cluster-wide Time")
    public long getClusterTime(){
        return cluster.getClusterTime();
    }

    @ManagedAnnotation("memberCount")
    @ManagedDescription("size of the cluster")
    public int getMemberCount(){
        return cluster.getMembers().size();
    }

    @ManagedAnnotation("Members")
    @ManagedDescription("List of Members")
    public List<String> getMembers(){
        Set<Member> members = cluster.getMembers();
        List<String> list = new ArrayList<String>(members.size());
        for (Member member: members){
            list.add(member.getInetSocketAddress().toString());
        }
        return list;
    }

    @ManagedAnnotation("running")
    @ManagedDescription("Running state")
    public boolean isRunning(){
        return managedObject.getLifecycleService().isRunning();
    }

    @ManagedAnnotation(value = "shutdown", operation = true)
    @ManagedDescription("Shutdown the Node")
    public void shutdown(){
        managedObject.getLifecycleService().shutdown();
    }

}
