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

package com.hazelcast.instance;

import com.hazelcast.management.TimedMemberStateFactory;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.PacketReader;
import com.hazelcast.nio.tcp.PacketWriter;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;

/**
 * NodeExtension is a <tt>Node</tt> extension mechanism to be able to plug different implementations of
 * some modules, like; <tt>SerializationService</tt>, <tt>SocketChannelWrapperFactory</tt> etc.
 *
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
     * @param type type of service
     * @return service implementation
     * @throws java.lang.IllegalArgumentException if type is not known
     */
    <T> T createService(Class<T> type);


    TimedMemberStateFactory getTimedMemberStateFactory();

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
     * Creates a <tt>PacketReader</tt> for given <tt>Connection</tt> instance.
     *
     * @param connection tcp-ip connection
     * @param ioService IOService
     *
     * @return packet reader
     */
    PacketReader createPacketReader(TcpIpConnection connection, IOService ioService);

    /**
     * Creates a <tt>PacketWriter</tt> for given <tt>Connection</tt> instance.
     *
     * @param connection tcp-ip connection
     * @param ioService IOService
     *
     * @return packet writer
     */
    PacketWriter createPacketWriter(TcpIpConnection connection, IOService ioService);

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
     * Destroys <tt>NodeExtension</tt>. Called on <tt>Node.shutdown()</tt>
     */
    void destroy();

}
