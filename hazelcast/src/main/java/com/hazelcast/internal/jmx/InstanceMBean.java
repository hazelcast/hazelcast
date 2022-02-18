/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.wan.impl.WanReplicationService;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.internal.jmx.ManagementService.quote;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Management bean for {@link com.hazelcast.core.HazelcastInstance}
 */
@ManagedDescription("HazelcastInstance")
@SuppressWarnings("checkstyle:methodcount")
public class InstanceMBean extends HazelcastMBean<HazelcastInstanceImpl> {

    private static final int INITIAL_CAPACITY = 3;

    final Config config;
    final Cluster cluster;
    private NodeMBean nodeMBean;
    private ManagedExecutorServiceMBean systemExecutorMBean;
    private ManagedExecutorServiceMBean asyncExecutorMBean;
    private ManagedExecutorServiceMBean scheduledExecutorMBean;
    private ManagedExecutorServiceMBean clientExecutorMBean;
    private ManagedExecutorServiceMBean clientQueryExecutorMBean;
    private ManagedExecutorServiceMBean clientBlockingExecutorMBean;
    private ManagedExecutorServiceMBean queryExecutorMBean;
    private ManagedExecutorServiceMBean ioExecutorMBean;
    private ManagedExecutorServiceMBean offloadableExecutorMBean;
    private PartitionServiceMBean partitionServiceMBean;
    private LoggingServiceMBean loggingServiceMBean;

    protected InstanceMBean(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService) {
        super(hazelcastInstance, managementService);
        createProperties(hazelcastInstance);
        this.config = hazelcastInstance.getConfig();
        this.cluster = hazelcastInstance.getCluster();
        Node node = hazelcastInstance.node;
        ExecutionService executionService = node.nodeEngine.getExecutionService();
        createMBeans(hazelcastInstance, managementService, node, executionService);
        registerMBeans();
        registerWanPublisherMBeans(node.nodeEngine.getWanReplicationService());
    }

    /**
     * Registers managed beans for all WAN publishers, if any.
     *
     * @param wanReplicationService the WAN replication service
     */
    private void registerWanPublisherMBeans(WanReplicationService wanReplicationService) {
        final Map<String, LocalWanStats> wanStats = wanReplicationService.getStats();
        if (wanStats == null) {
            return;
        }

        for (Entry<String, LocalWanStats> replicationStatsEntry : wanStats.entrySet()) {
            final String wanReplicationName = replicationStatsEntry.getKey();
            final LocalWanStats localWanStats = replicationStatsEntry.getValue();
            final Map<String, LocalWanPublisherStats> publisherStats = localWanStats.getLocalWanPublisherStats();

            for (String wanPublisherId : publisherStats.keySet()) {
                register(new WanPublisherMBean(wanReplicationService, wanReplicationName, wanPublisherId, service));
            }
        }
    }

    private void createMBeans(HazelcastInstanceImpl hazelcastInstance, ManagementService managementService, Node node,
                              ExecutionService executionService) {
        this.nodeMBean = new NodeMBean(hazelcastInstance, node, managementService);
        this.partitionServiceMBean = new PartitionServiceMBean(hazelcastInstance, node.partitionService, service);
        this.systemExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR), service);
        this.asyncExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR), service);
        this.scheduledExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR), service);
        this.clientExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR), service);
        this.clientQueryExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.CLIENT_QUERY_EXECUTOR), service);
        this.clientBlockingExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.CLIENT_BLOCKING_EXECUTOR), service);
        this.queryExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.QUERY_EXECUTOR), service);
        this.ioExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.IO_EXECUTOR), service);
        this.offloadableExecutorMBean = new ManagedExecutorServiceMBean(
                hazelcastInstance, executionService.getExecutor(ExecutionService.OFFLOADABLE_EXECUTOR), service);
        this.loggingServiceMBean = new LoggingServiceMBean(hazelcastInstance, service);
    }

    private void registerMBeans() {
        register(nodeMBean);
        register(partitionServiceMBean);
        register(systemExecutorMBean);
        register(asyncExecutorMBean);
        register(scheduledExecutorMBean);
        register(clientExecutorMBean);
        register(clientQueryExecutorMBean);
        register(clientBlockingExecutorMBean);
        register(queryExecutorMBean);
        register(ioExecutorMBean);
        register(offloadableExecutorMBean);
        register(loggingServiceMBean);
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

    public ManagedExecutorServiceMBean getClientQueryExecutorMBean() {
        return clientQueryExecutorMBean;
    }

    public ManagedExecutorServiceMBean getClientBlockingExecutorMBean() {
        return clientBlockingExecutorMBean;
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

    @ManagedAnnotation("clusterName")
    @ManagedDescription("Cluster Name")
    public String getClusterName() {
        return config.getClusterName();
    }

    @ManagedAnnotation("port")
    @ManagedDescription("Network Port")
    public int getPort() {
        return getActiveMemberNetworkConfig(config).getPort();
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
        List<String> list = new ArrayList<>(members.size());
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
