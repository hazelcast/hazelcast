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
import com.hazelcast.wan.WanReplicationService;

public interface NodeExtension {

    void beforeInitialize(Node node);

    void printNodeInfo(Node node);

    void afterInitialize(Node node);

    SerializationService createSerializationService();

    SecurityContext getSecurityContext();

    Storage<DataRef> getOffHeapStorage();

    WanReplicationService geWanReplicationService();

    MemberSocketInterceptor getMemberSocketInterceptor();

    SocketChannelWrapperFactory getSocketChannelWrapperFactory();

    PacketReader createPacketReader(TcpIpConnection connection, IOService ioService);

    PacketWriter createPacketWriter(TcpIpConnection connection, IOService ioService);

    void destroy();

    void onThreadStart(Thread thread);

    void onThreadStop(Thread thread);
}
