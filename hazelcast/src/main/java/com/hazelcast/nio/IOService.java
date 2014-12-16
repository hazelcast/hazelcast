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

package com.hazelcast.nio;

import com.hazelcast.ascii.TextCommandService;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.tcp.PacketReader;
import com.hazelcast.nio.tcp.PacketWriter;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.EventService;

import java.util.Collection;

public interface IOService {

    int KILO_BYTE = 1024;

    boolean isActive();

    ILogger getLogger(String name);

    void onOutOfMemory(OutOfMemoryError oom);

    Address getThisAddress();

    void onFatalError(Exception e);

    SocketInterceptorConfig getSocketInterceptorConfig();

    SymmetricEncryptionConfig getSymmetricEncryptionConfig();

    SSLConfig getSSLConfig();

    void handleMemberPacket(Packet p);

    void handleClientPacket(Packet p);

    TextCommandService getTextCommandService();

    boolean isMemcacheEnabled();

    boolean isRestEnabled();

    void removeEndpoint(Address endpoint);

    String getThreadPrefix();

    ThreadGroup getThreadGroup();

    void onSuccessfulConnection(Address address);

    void onFailedConnection(Address address);

    void shouldConnectTo(Address address);

    boolean isSocketBind();

    boolean isSocketBindAny();

    int getSocketReceiveBufferSize();

    int getSocketSendBufferSize();

    int getSocketLingerSeconds();

    int getSocketConnectTimeoutSeconds();

    boolean getSocketKeepAlive();

    boolean getSocketNoDelay();

    int getSelectorThreadCount();

    long getConnectionMonitorInterval();

    int getConnectionMonitorMaxFaults();

    void onDisconnect(Address endpoint);

    boolean isClient();

    void executeAsync(Runnable runnable);

    EventService getEventService();

    Collection<Integer> getOutboundPorts();

    Data toData(Object obj);

    Object toObject(Data data);

    SerializationService getSerializationService();

    PortableContext getPortableContext();

    SocketChannelWrapperFactory getSocketChannelWrapperFactory();

    MemberSocketInterceptor getMemberSocketInterceptor();

    PacketReader createPacketReader(TcpIpConnection connection);

    PacketWriter createPacketWriter(TcpIpConnection connection);
}
