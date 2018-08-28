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

package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.function.Consumer;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

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
    public void onFatalError(Exception e) {
        logger.severe("Fatal error", e);
    }

    @Override
    public SymmetricEncryptionConfig getSymmetricEncryptionConfig() {
        return null;
    }

    @Override
    public SSLConfig getSSLConfig() {
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
    public void interceptSocket(Socket socket, boolean onAccept) {
    }

    @Override
    public boolean isSocketInterceptorEnabled() {
        return false;
    }

    @Override
    public int getSocketConnectTimeoutSeconds() {
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
            public EventRegistration registerLocalListener(String serviceName, String topic, Object listener) {
                return null;
            }

            @Override
            public EventRegistration registerLocalListener(String serviceName, String topic, EventFilter filter, Object listener) {
                return null;
            }

            @Override
            public EventRegistration registerListener(String serviceName, String topic, Object listener) {
                return null;
            }

            @Override
            public EventRegistration registerListener(String serviceName, String topic, EventFilter filter, Object listener) {
                return null;
            }

            @Override
            public boolean deregisterListener(String serviceName, String topic, Object id) {
                return false;
            }

            @Override
            public void deregisterAllListeners(String serviceName, String topic) {
            }

            @Override
            public Collection<EventRegistration> getRegistrations(String serviceName, String topic) {
                return null;
            }

            @Override
            public EventRegistration[] getRegistrationsAsArray(String serviceName, String topic) {
                return new EventRegistration[0];
            }

            @Override
            public boolean hasEventRegistration(String serviceName, String topic) {
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
            public void executeEventCallback(Runnable callback) {
                new Thread(callback).start();
            }
        };
    }

    @Override
    public Collection<Integer> getOutboundPorts() {
        return Collections.emptyList();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return null;
    }

    @Override
    public InboundHandler[] createMemberInboundHandlers(final TcpIpConnection connection) {
        return new InboundHandler[]{new PacketDecoder(connection, new Consumer<Packet>() {
            @Override
            public void accept(Packet packet) {
                try {
                    if (packet.getPacketType() == Packet.Type.BIND) {
                        connection.getConnectionManager().accept(packet);
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
    public OutboundHandler[] createMemberOutboundHandlers(TcpIpConnection connection) {
        return new OutboundHandler[]{new PacketEncoder()};
    }
}
