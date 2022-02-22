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

package com.hazelcast.instance.impl;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.util.ByteArrayProcessor;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.version.Version;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * NodeExtension is a <tt>Node</tt> extension mechanism to be able to plug different implementations of
 * some modules, like; <tt>SerializationService</tt>, <tt>ChannelFactory</tt> etc.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public interface NodeExtension {

    /**
     * Called before node attempts to join to the cluster
     */
    default void beforeJoin() {
    }

    /**
     * Shutdowns <tt>NodeExtension</tt>. Called on <tt>Node.shutdown()</tt>
     * right after setting the state to PASSIVE.
     */
    default void shutdown() {
    }

    /**
     * Returns the security service if security context is available.
     * Returns {@code null} by default.
     */
    default SecurityService getSecurityService() {
        return null;
    }

    /**
     * Called on thread start to inject/intercept extension specific logic,
     * like; registering thread in some service,
     * executing a special method before thread starts to do its own task.
     *
     * @param thread thread starting
     */
    default void onThreadStart(Thread thread) {
    }

    /**
     * Called before a thread stops to clean/release injected by {@link #onThreadStart(Thread)}.
     *
     * @param thread thread stopping
     */
    default void onThreadStop(Thread thread) {
    }

    /**
     * Called when initial cluster state is received while joining the cluster.
     *
     * @param initialState initial cluster state
     */
    default void onInitialClusterState(ClusterState initialState) {
    }

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
    default void afterClusterStateChange(ClusterState oldState, ClusterState newState, boolean isTransient) {
    }

    /**
     * Creates a UUID for local member
     * @return new UUID
     */
    default UUID createMemberUuid() {
        return UuidUtil.newUnsecureUUID();
    }

    /** Returns a byte array processor for incoming data on the Multicast joiner */
    default ByteArrayProcessor createMulticastInputProcessor(ServerContext serverContext) {
        return null;
    }

    /** Returns a byte array processor for outgoing data on the Multicast joiner */
    default ByteArrayProcessor createMulticastOutputProcessor(ServerContext serverContext) {
        return null;
    }

    /**
     * Cluster version auto upgrade is done asynchronously. Every call of this
     * method creates and schedules a new auto upgrade task.
     */
    default void scheduleClusterVersionAutoUpgrade() {
    }

    /**
     * @return true if client failover feature is supported
     */
    default boolean isClientFailoverSupported() {
        return false;
    }

    /**
     * Called before node is started
     */
    void beforeStart();

    /**
     * Called to print node information during startup
     */
    void printNodeInfo();

    /**
     * Logs metadata about the instance to the configured instance tracking output.
     */
    void logInstanceTrackingMetadata();

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
    void beforeShutdown(boolean terminate);

    /**
     * Shutdowns <tt>NodeExtension</tt>. Called on <tt>Node.shutdown()</tt>
     * right before setting the state to SHUT_DOWN.
     */
    void afterShutdown();

    /**
     * Creates a <tt>SerializationService</tt> instance to be used by this <tt>Node</tt>.
     *
     * @return a <tt>SerializationService</tt> instance
     */
    InternalSerializationService createSerializationService();

    /**
     * Creates and returns a serialization service for (de)serializing objects
     * compatible with a compatibility (3.x) format.
     *
     * @return the compatibility serialization service
     */
    InternalSerializationService createCompatibilitySerializationService();

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
     * @param params additional parameter to create the service
     * @return service implementation
     * @throws java.lang.IllegalArgumentException if type is not known
     */
    <T> T createService(Class<T> type, Object... params);

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
     * @param endpointQualifier an endpoint qualifier that identifies
     *                         groups of network connections sharing a
     *                         common ProtocolType and the same network
     *                         settings
     * @return MemberSocketInterceptor
     */
    MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier);

    /**
     * Creates a <tt>InboundHandler</tt> for given <tt>Connection</tt> instance.
     *
     * For TLS and other enterprise features, instead of returning the regular protocol decoder, a TLS decoder
     * can be returned. This is the first item in the chain.
     *
     * @param connection tcp-ip connection
     * @param context a server context
     * @return the created InboundHandler.
     */
    InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, ServerConnection connection, ServerContext context);

    /**
     * Creates a <tt>OutboundHandler</tt> for given <tt>Connection</tt> instance.
     *
     * @param connection tcp-ip connection
     * @param context  ServerContext
     * @return the created OutboundHandler
     */
    OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, ServerConnection connection, ServerContext context);

    /**
     * Creates the channel initializer function.
     *
     * @param serverContext
     * @return
     */
    Function<EndpointQualifier, ChannelInitializer> createChannelInitializerFn(ServerContext serverContext);

    /**
     * Returns MemoryStats of for the JVM and current HazelcastInstance.
     *
     * @return memory statistics
     */
    MemoryStats getMemoryStats();

     /**
      * Executed on the master node before allowing a new member to join from
      * {@link com.hazelcast.internal.cluster.impl.ClusterJoinManager#handleJoinRequest(JoinRequest, ServerConnection)}.
      * Implementation should check if the {@code JoinMessage} should be allowed to proceed, otherwise throw an exception
      * with a message explaining rejection reason.
      */
    void validateJoinRequest(JoinMessage joinMessage);

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

    /**
     * Returns the public hot restart service
     */
    HotRestartService getHotRestartService();

    /** Returns the internal hot restart service */
    InternalHotRestartService getInternalHotRestartService();

    /**
     * Creates a TimedMemberStateFactory for a given Hazelcast instance
     * @param instance The instance to associate with the timed member state factory
     * @return {@link TimedMemberStateFactory}
     */
    TimedMemberStateFactory createTimedMemberStateFactory(HazelcastInstanceImpl instance);

    ManagementService createJMXManagementService(HazelcastInstanceImpl instance);

    TextCommandService createTextCommandService();

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
     * @return not-{@code null} {@link AuditlogService} instance
     */
    AuditlogService getAuditlogService();

    /**
     * Returns CP persistence service.
     */
    CPPersistenceService getCPPersistenceService();

    /**
     * Returns a JetService.
     */
    JetService getJet();

    /** Returns the internal jet service backend */
    @Nullable
    JetServiceBackend getJetServiceBackend();
}
