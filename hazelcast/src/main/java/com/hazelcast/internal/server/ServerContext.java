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

package com.hazelcast.internal.server;

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.io.IOException;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Contains many of the dependencies passed to the {@link Server}.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public interface ServerContext {

    int KILO_BYTE = 1024;

    boolean isNodeActive();

    HazelcastProperties properties();

    String getHazelcastName();

    LoggingService getLoggingService();

    // returns the MEMBER server socket address
    Address getThisAddress();

    /**
     * Returns UUID of the local member.
     *
     * @return member UUID
     */
    UUID getThisUuid();

    /**
     * @return all server socket addresses of this Hazelcast member, as picked by the
     * configured {@link com.hazelcast.instance.AddressPicker}
     */
    Map<EndpointQualifier, Address> getThisAddresses();

    void onFatalError(Exception e);

    SymmetricEncryptionConfig getSymmetricEncryptionConfig(EndpointQualifier endpointQualifier);

    /**
     * Returns initialized {@link RestApiConfig} for the node.
     */
    RestApiConfig getRestApiConfig();

    /**
     * Returns initialized {@link MemcacheProtocolConfig} for the node.
     */
    MemcacheProtocolConfig getMemcacheProtocolConfig();

    ClientEngine getClientEngine();

    TextCommandService getTextCommandService();

    void removeEndpoint(Address endpointAddress);

    void onSuccessfulConnection(Address address);

    void onFailedConnection(Address address);

    void shouldConnectTo(Address address);

    void interceptSocket(EndpointQualifier endpointQualifier, Socket socket, boolean onAccept) throws IOException;

    boolean isSocketInterceptorEnabled(EndpointQualifier endpointQualifier);

    int getSocketConnectTimeoutSeconds(EndpointQualifier endpointQualifier);

    long getConnectionMonitorInterval();

    int getConnectionMonitorMaxFaults();

    void onDisconnect(Address endpoint, Throwable cause);

    Future<Void> submitAsync(Runnable runnable);

    EventService getEventService();

    Collection<Integer> getOutboundPorts(EndpointQualifier endpointQualifier);

    InternalSerializationService getSerializationService();

    MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier);

    InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, ServerConnection connection);

    OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, ServerConnection connection);

    AuditlogService getAuditLogService();
}
