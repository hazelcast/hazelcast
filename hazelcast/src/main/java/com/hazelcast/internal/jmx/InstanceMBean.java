/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Management bean for {@link com.hazelcast.core.HazelcastInstance}
 */
@ManagedDescription("HazelcastInstance")
public class InstanceMBean extends HazelcastMBean<HazelcastInstanceImpl> {

    private static final int INITIAL_CAPACITY = 3;

    final Config config;
    final Cluster cluster;
    private NodeMBean nodeMBean;
    private ConnectionManagerMBean connectionManagerMBean;
    private EventServiceMBean eventServiceMBean;
    private OperationServiceMBean operationServiceMBean;
    private ProxyServiceMBean proxyServiceMBean;
    private ClientEngineMBean clientEngineMBean;
    private ManagedExecutorServiceMBean systemExecutorMBean;
    private ManagedExecutorServiceMBean asyncExecutorMBean;
    private ManagedExecutorServiceMBean scheduledExecutorMBean;
    private ManagedExecutorServiceMBean clientExecutorMBean;
    private ManagedExecutorServiceMBean queryExecutorMBean;
    private ManagedExecutorServiceMBean ioExecutorMBean;
    private ManagedExecutorServiceMBean offloadableExecutorMBean;
    private PartitionServiceMBean partitionServiceMBean;

    protected InstanceMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
        super(hazelcastInstance, managementService);
        createProperties(hazelcastInstance);
        this.config = hazelcastInstance.getConfig();
        this.cluster = hazelcastInstance.getCluster();
        Node node = hazelcastInstance.node;
        ExecutionService executionService = node.nodeEngine.getExecutionService();
        InternalOperationService operationService = node.nodeEngine.getOperationService();
        createMBeans(hazelcastInstance, managementService, node, executionService, operationService);
        registerMBeans();
    }

    private void createMBeans(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService, Node node,
                              ExecutionService executionService, InternalOperationService operationService) {
        this.nodeMBean = new NodeMBean(hazelcastInstance, node, managementService);
        this.connectionManagerMBean = new ConnectionManagerMBean(hazelcastInstance, node.connectionManager, service);
        this.eventServiceMBean = new EventServiceMBean(hazelcastInstance, node.nodeEngine.getEventService(), service);
        this.operationServiceMBean = new OperationServiceMBean(hazelcastInstance, operationService, service);
        this.proxyServiceMBean = new ProxyServiceMBean(hazelcastInstance, node.nodeEngine.getProxyService(), service);
        this.partitionServiceMBean = new PartitionServiceMBean(hazelcastInstance, node.partitionService, service);
        this.clientEngineMBean = new ClientEngineMBean(hazelcastInstance, node.clientEngine, service);
        this.systemExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR), service);
        this.asyncExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR), service);
        this.scheduledExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR), service);
        this.clientExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR), service);
        this.queryExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.QUERY_EXECUTOR), service);
        this.ioExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.IO_EXECUTOR), service);
        this.offloadableExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.OFFLOADABLE_EXECUTOR), service);
    }

    private void registerMBeans() {
        register(operationServiceMBean);
        register(nodeMBean);
        register(connectionManagerMBean);
        register(eventServiceMBean);
        register(proxyServiceMBean);
        register(partitionServiceMBean);
        register(clientEngineMBean);
        register(systemExecutorMBean);
        register(asyncExecutorMBean);
        register(scheduledExecutorMBean);
        register(clientExecutorMBean);
        register(queryExecutorMBean);
        register(ioExecutorMBean);
        register(offloadableExecutorMBean);
    }

    private void createProperties(HazelcastInstanceImpl hazelcastInstance) {
        final Map<String, String> properties = createHashMap(INITIAL_CAPACITY);
        properties.put("type", quote("HazelcastInstance"));
        properties.put("instance", quote(hazelcastInstance.getName()));
        properties.put("name", quote(hazelcastInstance.getName()));
        setObjectName(properties);
    }

    public PartitionServiceMBean getPartitionServiceMBean() {
        return partitionServiceMBean;
    }

    public ManagedExecutorServiceMBean getSystemExecutorMBean() {
        return systemExecutorMBean;
    }

    public ManagedExecutorServiceMBean getAsyncExecutorMBean() {
        return asyncExecutorMBean;
    }

    public ManagedExecutorServiceMBean getScheduledExecutorMBean() {
        return scheduledExecutorMBean;
    }

    public ManagedExecutorServiceMBean getClientExecutorMBean() {
        return clientExecutorMBean;
    }

    public ManagedExecutorServiceMBean getQueryExecutorMBean() {
        return queryExecutorMBean;
    }

    public ManagedExecutorServiceMBean getIoExecutorMBean() {
        return ioExecutorMBean;
    }

    public ManagedExecutorServiceMBean getOffloadableExecutorMBean() {
        return offloadableExecutorMBean;
    }

    public OperationServiceMBean getOperationServiceMBean() {
        return operationServiceMBean;
    }

    public ProxyServiceMBean getProxyServiceMBean() {
        return proxyServiceMBean;
    }

    public ClientEngineMBean getClientEngineMBean() {
        return clientEngineMBean;
    }

    public ConnectionManagerMBean getConnectionManagerMBean() {
        return connectionManagerMBean;
    }

    public EventServiceMBean getEventServiceMBean() {
        return eventServiceMBean;
    }

    public NodeMBean getNodeMBean() {
        return nodeMBean;
    }

    public HazelcastInstance getHazelcastInstance() {
        return managedObject;
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the Instance")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("version")
    @ManagedDescription("The Hazelcast version")
    public String getVersion() {
        return managedObject.node.getBuildInfo().getVersion();
    }

    @ManagedAnnotation("build")
    @ManagedDescription("The Hazelcast build")
    public String getBuild() {
        return managedObject.node.getBuildInfo().getBuild();
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
    public String getGroupName() {
        return config.getGroupConfig().getName();
    }

    @ManagedAnnotation("port")
    @ManagedDescription("Network Port")
    public int getPort() {
        return config.getNetworkConfig().getPort();
    }

    @ManagedAnnotation("clusterTime")
    @ManagedDescription("Cluster-wide Time")
    public long getClusterTime() {
        return cluster.getClusterTime();
    }

    @ManagedAnnotation("memberCount")
    @ManagedDescription("size of the cluster")
    public int getMemberCount() {
        return cluster.getMembers().size();
    }

    @ManagedAnnotation("Members")
    @ManagedDescription("List of Members")
    public List<String> getMembers() {
        Set<Member> members = cluster.getMembers();
        List<String> list = new ArrayList<String>(members.size());
        for (Member member : members) {
            list.add(member.getSocketAddress().toString());
        }
        return list;
    }

    @ManagedAnnotation("running")
    @ManagedDescription("Running state")
    public boolean isRunning() {
        return managedObject.getLifecycleService().isRunning();
    }

    @ManagedAnnotation(value = "shutdown", operation = true)
    @ManagedDescription("Shutdown the Node")
    public void shutdown() {
        managedObject.getLifecycleService().shutdown();
    }

}
