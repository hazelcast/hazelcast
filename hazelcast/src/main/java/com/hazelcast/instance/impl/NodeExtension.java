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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.auditlog.AuditlogService;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.ByteArrayProcessor;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.version.Version;

import java.util.Map;
import java.util.UUID;

/**
 * NodeExtension is a <tt>Node</tt> extension mechanism to be able to plug different implementations of
 * some modules, like; <tt>SerializationService</tt>, <tt>ChannelFactory</tt> etc.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public interface NodeExtension {

    /**
     * Called before node is started
     */
    void beforeStart();

    /**
     * Called to print node information during startup
     */
    void printNodeInfo();

    /**
     * Called before node attempts to join to the cluster
     */
    void beforeJoin();

    /**
     * Called after node is started
     */
    void afterStart();

    /**
     * Returns true if the instance has started
     */
    boolean isStartCompleted();

    /**
     * Called before <tt>Node.shutdown()</tt>
     */
    void beforeShutdown();

    /**
     * Shutdowns <tt>NodeExtension</tt>. Called on <tt>Node.shutdown()</tt>
     */
    void shutdown();

    /**
     * Creates a <tt>SerializationService</tt> instance to be used by this <tt>Node</tt>.
     *
     * @return a <tt>SerializationService</tt> instance
     */
    InternalSerializationService createSerializationService();

    SecurityService getSecurityService();

    /**
     * Returns <tt>SecurityContext</tt> for this <tt>Node</tt> if available, otherwise returns null.
     *
     * @return security context
     */
    SecurityContext getSecurityContext();

    /**
     * Creates a service which is an implementation of given type parameter.
     *
     * @param type type of service
     * @return service implementation
     * @throws java.lang.IllegalArgumentException if type is not known
     */
    <T> T createService(Class<T> type);

    /**
     * Creates additional extension services, which will be registered by
     * service manager during start-up.
     *
     * By default returned map will be empty.
     *
     * @return extension services
     */
    Map<String, Object> createExtensionServices();

    /**
     * Returns <tt>MemberSocketInterceptor</tt> for this <tt>Node</tt> if available,
     * otherwise returns null.
     *
     * @return MemberSocketInterceptor
     * @param endpointQualifier
     */
    MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier);

    /**
     * Creates a <tt>InboundHandler</tt> for given <tt>Connection</tt> instance.
     *
     * For TLS and other enterprise features, instead of returning the regular protocol decoder, a TLS decoder
     * can be returned. This is the first item in the chain.
     *
     * @param connection tcp-ip connection
     * @param ioService  IOService
     * @return the created InboundHandler.
     */
    InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, TcpIpConnection connection, IOService ioService);

    /**
     * Creates a <tt>OutboundHandler</tt> for given <tt>Connection</tt> instance.
     *
     * @param connection tcp-ip connection
     * @param ioService  IOService
     * @return the created OutboundHandler
     */
    OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, TcpIpConnection connection, IOService ioService);


    /**
     * Creates the ChannelInitializerProvider.
     *
     * @param ioService
     * @return
     */
    ChannelInitializerProvider createChannelInitializerProvider(IOService ioService);

    /**
     * Called on thread start to inject/intercept extension specific logic,
     * like; registering thread in some service,
     * executing a special method before thread starts to do its own task.
     *
     * @param thread thread starting
     */
    void onThreadStart(Thread thread);

    /**
     * Called before a thread stops to clean/release injected by {@link #onThreadStart(Thread)}.
     *
     * @param thread thread stopping
     */
    void onThreadStop(Thread thread);

    /**
     * Returns MemoryStats of for the JVM and current HazelcastInstance.
     *
     * @return memory statistics
     */
    MemoryStats getMemoryStats();

     /**
      * Executed on the master node before allowing a new member to join from
      * {@link com.hazelcast.internal.cluster.impl.ClusterJoinManager#handleJoinRequest(JoinRequest, Connection)}.
      * Implementation should check if the {@code JoinMessage} should be allowed to proceed, otherwise throw an exception
      * with a message explaining rejection reason.
      */
    void validateJoinRequest(JoinMessage joinMessage);

    /**
     * Called when initial cluster state is received while joining the cluster.
     *
     * @param initialState initial cluster state
     */
    void onInitialClusterState(ClusterState initialState);

    /**
     * Called before starting a cluster state change transaction. Called only
     * on the member that initiated the state change.
     *
     * @param currState the state before the change
     * @param requestedState the requested cluster state
     * @param isTransient whether the change will be recorded in persistent storage, affecting the
     *                    initial state after cluster restart. Transient changes happen during
     *                    system operations such as an orderly all-cluster shutdown.
     */
    void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient);

    /**
     * Called during the commit phase of the cluster state change transaction,
     * just after updating the value of the cluster state on the local member,
     * while still holding the cluster lock. Called on all cluster members.
     *
     * @param newState the new cluster state
     * @param isTransient whether the change will be recorded in persistent storage, affecting the
     *                    initial state after cluster restart. Transient changes happen during
     *                    system operations such as an orderly all-cluster shutdown.
     */
    void onClusterStateChange(ClusterState newState, boolean isTransient);

    /**
     * Called after the cluster state change transaction has completed
     * (successfully or otherwise). Called only on the member that initiated
     * the state change.
     *
     * @param oldState the state before the change
     * @param newState the new cluster state, can be equal to {@code oldState} if the
     *                 state change transaction failed
     * @param isTransient whether the change will be recorded in persistent storage, affecting the
     *                    initial state after cluster restart. Transient changes happen during
     *                    system operations such as an orderly all-cluster shutdown.
     */
    void afterClusterStateChange(ClusterState oldState, ClusterState newState, boolean isTransient);

    /**
     * Called synchronously when partition state (partition assignments, version etc) changes
     */
    void onPartitionStateChange();

    /**
     * Called synchronously when member list changes
     */
    void onMemberListChange();

    /**
     * Called after cluster version is changed.
     *
     * @param newVersion the new version at which the cluster operates.
     */
    void onClusterVersionChange(Version newVersion);

    /**
     * Check if this node's codebase version is compatible with given cluster version.
     * @param clusterVersion the cluster version to check against
     * @return {@code true} if compatible, otherwise false.
     */
    boolean isNodeVersionCompatibleWith(Version clusterVersion);

    /**
     * Registers given listener if it's a known type.
     * @param listener listener instance
     * @return true if listener is registered, false otherwise
     */
    boolean registerListener(Object listener);

    /** Returns the public hot restart service */
    HotRestartService getHotRestartService();

    /** Returns the internal hot restart service */
    InternalHotRestartService getInternalHotRestartService();

    /**
     * Creates a UUID for local member
     * @return new UUID
     */
    UUID createMemberUuid();

    /**
     * Creates a TimedMemberStateFactory for a given Hazelcast instance
     * @param instance The instance to associate with the timed member state factory
     * @return {@link TimedMemberStateFactory}
     */
    TimedMemberStateFactory createTimedMemberStateFactory(HazelcastInstanceImpl instance);

    ManagementCenterConnectionFactory getManagementCenterConnectionFactory();

    ManagementService createJMXManagementService(HazelcastInstanceImpl instance);

    TextCommandService createTextCommandService();

    /** Returns a byte array processor for incoming data on the Multicast joiner */
    ByteArrayProcessor createMulticastInputProcessor(IOService ioService);

    /** Returns a byte array processor for outgoing data on the Multicast joiner */
    ByteArrayProcessor createMulticastOutputProcessor(IOService ioService);

    /**
     * Creates a listener for changes in dynamic data structure configurations
     *
     * @return Listener to be notfied about changes in data structure configurations
     */
    DynamicConfigListener createDynamicConfigListener();

    /**
     * Register the node extension specific diagnostics plugins on the provided
     * {@code diagnostics}.
     *
     * @param diagnostics the diagnostics on which plugins should be registered
     */
    void registerPlugins(Diagnostics diagnostics);

    /**
     * Send PhoneHome ping from OS or EE instance to PhoneHome application
     */
    void sendPhoneHome();

    /**
     * Cluster version auto upgrade is done asynchronously. Every call of this
     * method creates and schedules a new auto upgrade task.
     */
    void scheduleClusterVersionAutoUpgrade();

    /**
     * @return true if client failover feature is supported
     */
    boolean isClientFailoverSupported();

    /**
     * @return not-{@code null} {@link AuditlogService} instance
     */
    AuditlogService getAuditlogService();

    /**
     * Returns CP persistence service.
     */
    CPPersistenceService getCPPersistenceService();
}
