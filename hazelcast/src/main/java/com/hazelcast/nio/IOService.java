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

package com.hazelcast.nio;

import com.hazelcast.client.ClientEngine;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;

@PrivateApi
public interface IOService {

    int KILO_BYTE = 1024;

    boolean isActive();

    String getHazelcastName();

    LoggingService getLoggingService();

    Address getThisAddress();

    void onFatalError(Exception e);

    SymmetricEncryptionConfig getSymmetricEncryptionConfig();

    SSLConfig getSSLConfig();

    ClientEngine getClientEngine();

    TextCommandService getTextCommandService();

    boolean isMemcacheEnabled();

    boolean isRestEnabled();

    boolean isHealthcheckEnabled();

    void removeEndpoint(Address endpoint);

    void onSuccessfulConnection(Address address);

    void onFailedConnection(Address address);

    void shouldConnectTo(Address address);

    boolean isSocketBind();

    boolean isSocketBindAny();

    int getSocketReceiveBufferSize();

    int getSocketSendBufferSize();

    boolean useDirectSocketBuffer();

    /**
     * Size of receive buffers for connections opened by clients
     *
     * @return size in bytes
     */
    int getSocketClientReceiveBufferSize();

    /**
     * Size of send buffers for connections opened by clients
     *
     * @return size in bytes
     */
    int getSocketClientSendBufferSize();

    void configureSocket(Socket socket) throws SocketException;

    void interceptSocket(Socket socket, boolean onAccept) throws IOException;

    boolean isSocketInterceptorEnabled();

    int getSocketConnectTimeoutSeconds();

    int getInputSelectorThreadCount();

    int getOutputSelectorThreadCount();

    long getConnectionMonitorInterval();

    int getConnectionMonitorMaxFaults();

    /**
     * @return Time interval between two I/O imbalance checks.
     */
    int getBalancerIntervalSeconds();

    void onDisconnect(Address endpoint, Throwable cause);

    void executeAsync(Runnable runnable);

    EventService getEventService();

    Collection<Integer> getOutboundPorts();

    InternalSerializationService getSerializationService();

    ChannelFactory getChannelFactory();

    MemberSocketInterceptor getMemberSocketInterceptor();

    ChannelInboundHandler createInboundHandler(TcpIpConnection connection);

    ChannelOutboundHandler createOutboundHandler(TcpIpConnection connection);
}
