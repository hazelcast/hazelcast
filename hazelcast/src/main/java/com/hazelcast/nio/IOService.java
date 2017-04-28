/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.networking.IOOutOfMemoryHandler;
import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.internal.networking.SocketChannelWrapperFactory;
import com.hazelcast.internal.networking.WriteHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;

@PrivateApi
public interface IOService {

    int KILO_BYTE = 1024;

    boolean isActive();

    String getHazelcastName();

    LoggingService getLoggingService();

    IOOutOfMemoryHandler getIoOutOfMemoryHandler();

    Address getThisAddress();

    void onFatalError(Exception e);

    SocketInterceptorConfig getSocketInterceptorConfig();

    SymmetricEncryptionConfig getSymmetricEncryptionConfig();

    SSLConfig getSSLConfig();

    void handleClientMessage(ClientMessage cm, Connection connection);

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

    boolean isSocketBufferDirect();

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

    int getSocketLingerSeconds();

    int getSocketConnectTimeoutSeconds();

    boolean getSocketKeepAlive();

    boolean getSocketNoDelay();

    int getInputSelectorThreadCount();

    int getOutputSelectorThreadCount();

    long getConnectionMonitorInterval();

    int getConnectionMonitorMaxFaults();

    /**
     * @return Time interval between two I/O imbalance checks.
     */
    int getBalancerIntervalSeconds();

    void onDisconnect(Address endpoint, Throwable cause);

    boolean isClient();

    void executeAsync(Runnable runnable);

    EventService getEventService();

    Collection<Integer> getOutboundPorts();

    InternalSerializationService getSerializationService();

    SocketChannelWrapperFactory getSocketChannelWrapperFactory();

    MemberSocketInterceptor getMemberSocketInterceptor();

    ReadHandler createReadHandler(TcpIpConnection connection);

    WriteHandler createWriteHandler(TcpIpConnection connection);
}
