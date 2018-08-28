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

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.net.Socket;
import java.util.Collection;

@PrivateApi
public interface IOService {

    int KILO_BYTE = 1024;

    boolean isActive();

    HazelcastProperties properties();

    String getHazelcastName();

    LoggingService getLoggingService();

    Address getThisAddress();

    void onFatalError(Exception e);

    SymmetricEncryptionConfig getSymmetricEncryptionConfig();

    SSLConfig getSSLConfig();

    ClientEngine getClientEngine();

    TextCommandService getTextCommandService();

    void removeEndpoint(Address endpoint);

    void onSuccessfulConnection(Address address);

    void onFailedConnection(Address address);

    void shouldConnectTo(Address address);

    boolean isSocketBind();

    boolean isSocketBindAny();

    void interceptSocket(Socket socket, boolean onAccept) throws IOException;

    boolean isSocketInterceptorEnabled();

    int getSocketConnectTimeoutSeconds();

    long getConnectionMonitorInterval();

    int getConnectionMonitorMaxFaults();

    void onDisconnect(Address endpoint, Throwable cause);

    void executeAsync(Runnable runnable);

    EventService getEventService();

    Collection<Integer> getOutboundPorts();

    InternalSerializationService getSerializationService();

    MemberSocketInterceptor getMemberSocketInterceptor();

    InboundHandler[] createMemberInboundHandlers(TcpIpConnection connection);

    OutboundHandler[] createMemberOutboundHandlers(TcpIpConnection connection);
}
