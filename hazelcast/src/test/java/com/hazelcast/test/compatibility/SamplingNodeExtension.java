/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.compatibility;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.internal.util.ByteArrayProcessor;
import com.hazelcast.version.Version;

import java.util.Map;

/**
 * Node extension that instantiates a {@link SamplingSerializationService} when asked to create
 * {@link SerializationService} instance.
 */
public class SamplingNodeExtension implements NodeExtension {

    private final NodeExtension nodeExtension;

    public SamplingNodeExtension(NodeExtension nodeExtension) {
        this.nodeExtension = nodeExtension;
    }

    @Override
    public InternalSerializationService createSerializationService() {
        InternalSerializationService serializationService = nodeExtension.createSerializationService();
        return new SamplingSerializationService(serializationService);
    }

    @Override
    public SecurityService getSecurityService() {
        return nodeExtension.getSecurityService();
    }

    @Override
    public void beforeStart() {
        nodeExtension.beforeStart();
    }

    @Override
    public void printNodeInfo() {
        nodeExtension.printNodeInfo();
    }

    @Override
    public void beforeJoin() {
        nodeExtension.beforeJoin();
    }

    @Override
    public void afterStart() {
        nodeExtension.afterStart();
    }

    @Override
    public boolean isStartCompleted() {
        return nodeExtension.isStartCompleted();
    }

    @Override
    public void beforeShutdown() {
        nodeExtension.beforeShutdown();
    }

    @Override
    public void shutdown() {
        nodeExtension.shutdown();
    }

    @Override
    public SecurityContext getSecurityContext() {
        return nodeExtension.getSecurityContext();
    }

    @Override
    public <T> T createService(Class<T> type) {
        return nodeExtension.createService(type);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        return nodeExtension.createExtensionServices();
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        return nodeExtension.getSocketInterceptor(endpointQualifier);
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, TcpIpConnection connection, IOService ioService) {
        return nodeExtension.createInboundHandlers(qualifier, connection, ioService);
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, TcpIpConnection connection, IOService ioService) {
        return nodeExtension.createOutboundHandlers(qualifier, connection, ioService);
    }

    @Override
    public ChannelInitializerProvider createChannelInitializerProvider(IOService ioService) {
        return nodeExtension.createChannelInitializerProvider(ioService);
    }

    @Override
    public void onThreadStart(Thread thread) {
        nodeExtension.onThreadStart(thread);
    }

    @Override
    public void onThreadStop(Thread thread) {
        nodeExtension.onThreadStop(thread);
    }

    @Override
    public MemoryStats getMemoryStats() {
        return nodeExtension.getMemoryStats();
    }

    @Override
    public void validateJoinRequest(JoinMessage joinMessage) {
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Override
    public void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient) {
        nodeExtension.beforeClusterStateChange(currState, requestedState, isTransient);
    }

    public void onInitialClusterState(ClusterState initialState) {
        nodeExtension.onInitialClusterState(initialState);
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        nodeExtension.onClusterStateChange(newState, isTransient);
    }

    @Override
    public void afterClusterStateChange(ClusterState oldState, ClusterState newState, boolean isTransient) {
        nodeExtension.afterClusterStateChange(oldState, newState, isTransient);
    }

    @Override
    public void onPartitionStateChange() {
        nodeExtension.onPartitionStateChange();
    }

    @Override
    public void onMemberListChange() {
        nodeExtension.onMemberListChange();
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        nodeExtension.onClusterVersionChange(newVersion);
    }

    @Override
    public boolean isNodeVersionCompatibleWith(Version clusterVersion) {
        return nodeExtension.isNodeVersionCompatibleWith(clusterVersion);
    }

    @Override
    public boolean registerListener(Object listener) {
        return nodeExtension.registerListener(listener);
    }

    @Override
    public HotRestartService getHotRestartService() {
        return nodeExtension.getHotRestartService();
    }

    @Override
    public InternalHotRestartService getInternalHotRestartService() {
        return nodeExtension.getInternalHotRestartService();
    }

    @Override
    public String createMemberUuid(Address address) {
        return nodeExtension.createMemberUuid(address);
    }

    @Override
    public TimedMemberStateFactory createTimedMemberStateFactory(HazelcastInstanceImpl instance) {
        return nodeExtension.createTimedMemberStateFactory(instance);
    }

    @Override
    public ManagementCenterConnectionFactory getManagementCenterConnectionFactory() {
        return nodeExtension.getManagementCenterConnectionFactory();
    }

    @Override
    public ByteArrayProcessor createMulticastInputProcessor(IOService ioService) {
        return nodeExtension.createMulticastInputProcessor(ioService);
    }

    @Override
    public ByteArrayProcessor createMulticastOutputProcessor(IOService ioService) {
        return nodeExtension.createMulticastOutputProcessor(ioService);
    }

    @Override
    public DynamicConfigListener createDynamicConfigListener() {
        return nodeExtension.createDynamicConfigListener();
    }

    @Override
    public void registerPlugins(Diagnostics diagnostics) {
    }

    @Override
    public ManagementService createJMXManagementService(HazelcastInstanceImpl instance) {
        return nodeExtension.createJMXManagementService(instance);
    }

    @Override
    public TextCommandService createTextCommandService() {
        return nodeExtension.createTextCommandService();
    }

    @Override
    public void sendPhoneHome() {
    }

    @Override
    public void scheduleClusterVersionAutoUpgrade() {
        nodeExtension.scheduleClusterVersionAutoUpgrade();
    }

    @Override
    public boolean isClientFailoverSupported() {
        return false;
    }
}
