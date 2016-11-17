/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.internal.networking.WriteHandler;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Map;
import java.util.Set;

/**
 * NodeExtension is a <tt>Node</tt> extension mechanism to be able to plug different implementations of
 * some modules, like; <tt>SerializationService</tt>, <tt>SocketChannelWrapperFactory</tt> etc.
 */
@PrivateApi
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
     */
    MemberSocketInterceptor getMemberSocketInterceptor();

    /**
     * Returns <tt>SocketChannelWrapperFactory</tt> instance to be used by this <tt>Node</tt>.
     *
     * @return SocketChannelWrapperFactory
     */
    SocketChannelWrapperFactory getSocketChannelWrapperFactory();

    /**
     * Creates a <tt>ReadHandler</tt> for given <tt>Connection</tt> instance.
     *
     * @param connection tcp-ip connection
     * @param ioService  IOService
     * @return the created ReadHandler.
     */
    ReadHandler createReadHandler(TcpIpConnection connection, IOService ioService);

    /**
     * Creates a <tt>WriteHandler</tt> for given <tt>Connection</tt> instance.
     *
     * @param connection tcp-ip connection
     * @param ioService  IOService
     * @return the created WriteHandler
     */
    WriteHandler createWriteHandler(TcpIpConnection connection, IOService ioService);

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
     * Called before a new node is joining to cluster,
     * executed if node is the master node before join event.
     * {@link com.hazelcast.internal.cluster.impl.ClusterJoinManager} calls this method,
     * when handleJoinRequest method is called. By this way, we can check the logic we want
     * by implementing this method. Implementation should throw required exception, with a valid
     * message which explains rejection reason.
     */
    void validateJoinRequest();

    /**
     * Called when cluster state is changed
     *
     * @param newState new state
     * @param isTransient status of the change. A cluster state change may be transient if it has been done temporarily
     *                         during system operations such cluster start etc.
     */
    void onClusterStateChange(ClusterState newState, boolean isTransient);

    /**
     * Called when partition state (partition assignments, version etc) changes
     */
    void onPartitionStateChange();

    /**
     * Registers given register if it's a known type.
     * @param listener listener instance
     * @return true if listener is registered, false otherwise
     */
    boolean registerListener(Object listener);

    /**
     * Forces node to start by skipping hot-restart completely and removing all hot-restart data
     * even if node is still on validation phase or loading hot-restart data.
     *
     * @return true if force start is triggered successfully. force start cannot be triggered if hot restart is disabled or
     * the master is not known yet
     */
    boolean triggerForceStart();

    /**
     * Triggers partial start if the cluster cannot be started with full recovery and
     * {@link HotRestartPersistenceConfig#clusterDataRecoveryPolicy} is set accordingly.
     *
     * @return true if partial start is triggered.
     */
    boolean triggerPartialStart();

    /**
     * Creates a UUID for local member
     * @param address address of local member
     * @return new uuid
     */
    String createMemberUuid(Address address);

    /**
     * Checks if the given member has been excluded during the cluster start or not.
     * If returns true, it means that the given member is not allowed to join to the cluster.
     *
     * @param memberAddress address of the member to check
     * @param memberUuid uuid of the member to check
     * @return true if the member has been excluded on cluster start.
     */
    boolean isMemberExcluded(Address memberAddress, String memberUuid);

    /**
     * Returns uuids of the members that have been excluded during the cluster start.
     *
     * @return uuids of the members that have been excluded during the cluster start
     */
    Set<String> getExcludedMemberUuids();

    /**
     * Handles the uuid set of excluded members only if this member is also excluded, and triggers the member force start process.
     *
     * @param sender the member that has sent the excluded members set
     * @param excludedMemberUuids uuids of the members that have been excluded during the cluster start
     */
    void handleExcludedMemberUuids(Address sender, Set<String> excludedMemberUuids);
}
