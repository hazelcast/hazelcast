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

package com.hazelcast.instance;

import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.storage.DataRef;
import com.hazelcast.internal.storage.Storage;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;

/**
 * NodeExtension is a <tt>Node</tt> extension mechanism to be able to plug different implementations of
 * some modules, like; <tt>SerializationService</tt>, <tt>SocketChannelWrapperFactory</tt> etc.
 */
public interface NodeExtension {

    /**
     * Called before node is started
     */
    void beforeStart(Node node);

    /**
     * Called to print node information during startup
     */
    void printNodeInfo(Node node);

    /**
     * Called after node is started
     */
    void afterStart(Node node);

    /**
     * Creates a <tt>SerializationService</tt> instance to be used by this <tt>Node</tt>.
     *
     * @return a <tt>SerializationService</tt> instance
     */
    SerializationService createSerializationService();

    /**
     * Returns <tt>SecurityContext</tt> for this <tt>Node</tt> if available, otherwise returns null.
     *
     * @return security context
     */
    SecurityContext getSecurityContext();

    /**
     * @deprecated
     */
    @Deprecated
    Storage<DataRef> getNativeDataStorage();

    /**
     * Creates a service which is an implementation of given type parameter.
     *
     * @param type type of service
     * @return service implementation
     * @throws java.lang.IllegalArgumentException if type is not known
     */
    <T> T createService(Class<T> type);

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
     * Creates factory method that creates server side client message handlers
     *
     * @param node node
     */
    MessageTaskFactory createMessageTaskFactory(Node node);

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
     * Destroys <tt>NodeExtension</tt>. Called on <tt>Node.shutdown()</tt>
     */
    void destroy();

    /**
     * Called before a new node is joining to cluster,
     * executed if node is the master node before join event.
     * {@link com.hazelcast.internal.cluster.impl.ClusterServiceImpl} calls this method,
     * when handleJoinRequest method is called. By this way, we can check the logic we want
     * by implementing this method. Implementation should throw required exception, with a valid
     * message which explains rejection reason.
     */
    void beforeJoin();
}
