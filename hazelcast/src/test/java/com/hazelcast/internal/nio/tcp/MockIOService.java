/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.auditlog.AuditlogService;
import com.hazelcast.internal.auditlog.impl.NoOpAuditlogService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.Type.BIND;
import static com.hazelcast.spi.properties.GroupProperty.IO_INPUT_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.IO_OUTPUT_THREAD_COUNT;

public class MockIOService implements IOService {

    public final ServerSocketChannel serverSocketChannel;
    public final Address thisAddress;
    public final InternalSerializationService serializationService;
    public final LoggingServiceImpl loggingService;
    public final ConcurrentHashMap<Long, DummyPayload> payloads = new ConcurrentHashMap<Long, DummyPayload>();
    private final HazelcastProperties properties;
    public volatile Consumer<Packet> packetConsumer;
    private final ILogger logger;

    public MockIOService(int port) throws Exception {
        loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo());
        logger = loggingService.getLogger(MockIOService.class);
        serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.setReuseAddress(true);
        serverSocket.setSoTimeout(1000);
        serverSocket.bind(new InetSocketAddress("0.0.0.0", port));
        thisAddress = new Address("127.0.0.1", port);
        this.serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();

        Properties props = new Properties();
        props.put(IO_INPUT_THREAD_COUNT.getName(), "1");
        props.put(IO_OUTPUT_THREAD_COUNT.getName(), "1");
        this.properties = new HazelcastProperties(props);
    }

    @Override
    public HazelcastProperties properties() {
        return properties;
    }

    @Override
    public String getHazelcastName() {
        return "hz";
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public LoggingService getLoggingService() {
        return loggingService;
    }

    @Override
    public Address getThisAddress() {
        return thisAddress;
    }

    @Override
    public Map<EndpointQualifier, Address> getThisAddresses() {
        Map<EndpointQualifier, Address> addressMap = new HashMap<EndpointQualifier, Address>();
        addressMap.put(EndpointQualifier.MEMBER, thisAddress);
        return addressMap;
    }

    @Override
    public void onFatalError(Exception e) {
        logger.severe("Fatal error", e);
    }

    @Override
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig(EndpointQualifier endpointQualifier) {
        return null;
    }

    @Override
    public SSLConfig getSSLConfig(EndpointQualifier endpointQualifier) {
        return null;
    }

    @Override
    public ClientEngine getClientEngine() {
        return null;
    }

    @Override
    public TextCommandService getTextCommandService() {
        return null;
    }

    @Override
    public void removeEndpoint(Address endpoint) {
        logger.info("Removing endpoint: " + endpoint);
    }

    @Override
    public void onSuccessfulConnection(Address address) {
        logger.info("Successful connection: " + address);
    }

    @Override
    public void onFailedConnection(Address address) {
        logger.info("Failed connection: " + address);
    }

    @Override
    public void shouldConnectTo(Address address) {
        if (thisAddress.equals(address)) {
            throw new RuntimeException("Connecting to self! " + address);
        }
    }

    @Override
    public boolean isSocketBind() {
        return true;
    }

    @Override
    public boolean isSocketBindAny() {
        return true;
    }

    @Override
    public void interceptSocket(EndpointQualifier endpointQualifier, Socket socket, boolean onAccept) {
    }

    @Override
    public boolean isSocketInterceptorEnabled(EndpointQualifier endpointQualifier) {
        return false;
    }

    @Override
    public int getSocketConnectTimeoutSeconds(EndpointQualifier endpointQualifier) {
        return 0;
    }

    @Override
    public long getConnectionMonitorInterval() {
        return 0;
    }

    @Override
    public int getConnectionMonitorMaxFaults() {
        return 0;
    }

    @Override
    public void onDisconnect(Address endpoint, Throwable cause) {
        logger.warning("Disconnected address: " + endpoint, cause);
    }

    @Override
    public void executeAsync(final Runnable runnable) {
        new Thread() {
            public void run() {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    logger.severe(t);
                }
            }
        }.start();
    }

    @Override
    public EventService getEventService() {
        return new EventService() {
            @Override
            public int getEventThreadCount() {
                return 0;
            }

            @Override
            public int getEventQueueCapacity() {
                return 0;
            }

            @Override
            public int getEventQueueSize() {
                return 0;
            }

            @Override
            public EventRegistration registerLocalListener(@Nonnull String serviceName,
                                                           @Nonnull String topic,
                                                           @Nonnull Object listener) {
                return null;
            }

            @Override
            public EventRegistration registerLocalListener(@Nonnull String serviceName,
                                                           @Nonnull String topic,
                                                           @Nonnull EventFilter filter,
                                                           @Nonnull Object listener) {
                return null;
            }

            @Override
            public EventRegistration registerListener(@Nonnull String serviceName,
                                                      @Nonnull String topic,
                                                      @Nonnull Object listener) {
                return null;
            }

            @Override
            public EventRegistration registerListener(@Nonnull String serviceName, @Nonnull String topic, @Nonnull EventFilter filter, @Nonnull Object listener) {
                return null;
            }

            @Override
            public boolean deregisterListener(@Nonnull String serviceName, @Nonnull String topic, @Nonnull Object id) {
                return false;
            }

            @Override
            public void deregisterAllListeners(@Nonnull String serviceName, @Nonnull String topic) {
            }

            @Override
            public Collection<EventRegistration> getRegistrations(@Nonnull String serviceName, @Nonnull String topic) {
                return null;
            }

            @Override
            public EventRegistration[] getRegistrationsAsArray(@Nonnull String serviceName, @Nonnull String topic) {
                return new EventRegistration[0];
            }

            @Override
            public boolean hasEventRegistration(@Nonnull String serviceName, @Nonnull String topic) {
                return false;
            }

            @Override
            public void publishEvent(String serviceName, String topic, Object event, int orderKey) {
            }

            @Override
            public void publishEvent(String serviceName, EventRegistration registration, Object event, int orderKey) {
            }

            @Override
            public void publishEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey) {
            }

            @Override
            public void publishRemoteEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey) {
            }

            @Override
            public void executeEventCallback(@Nonnull Runnable callback) {
                new Thread(callback).start();
            }

            @Override
            public void close(EventRegistration eventRegistration) {
            }

            @Override
            public Operation getPostJoinOperation() {
                return null;
            }

            @Override
            public Operation getPreJoinOperation() {
                return null;
            }

            @Override
            public void accept(Packet packet) {
            }
        };
    }

    @Override
    public Collection<Integer> getOutboundPorts(EndpointQualifier endpointQualifier) {
        return Collections.emptyList();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        return null;
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, final TcpIpConnection connection) {
        return new InboundHandler[]{new PacketDecoder(connection, new Consumer<Packet>() {
            @Override
            public void accept(Packet packet) {
                try {
                    if (packet.getPacketType() == BIND) {
                        connection.getEndpointManager().accept(packet);
                    } else {
                        Consumer<Packet> consumer = packetConsumer;
                        if (consumer != null) {
                            consumer.accept(packet);
                        }
                    }
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
        })};
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, TcpIpConnection connection) {
        return new OutboundHandler[]{new PacketEncoder()};
    }

    @Override
    public RestApiConfig getRestApiConfig() {
        return new RestApiConfig();
    }

    @Override
    public MemcacheProtocolConfig getMemcacheProtocolConfig() {
        return new MemcacheProtocolConfig();
    }

    @Override
    public AuditlogService getAuditLogService() {
        return NoOpAuditlogService.INSTANCE;
    }
}
