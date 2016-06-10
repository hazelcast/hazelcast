package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class MockIOService implements IOService {

    public final ServerSocketChannel serverSocketChannel;
    public final Address thisAddress;
    public final InternalSerializationService serializationService;
    public final LoggingServiceImpl loggingService;
    public final HazelcastThreadGroup hazelcastThreadGroup;
    public final ConcurrentHashMap<Long, DummyPayload> payloads = new ConcurrentHashMap<Long, DummyPayload>();
    public volatile PacketHandler packetHandler;

    public MockIOService(int port) throws Exception {
        loggingService = new LoggingServiceImpl("somegroup", "log4j", BuildInfoProvider.getBuildInfo());
        hazelcastThreadGroup = new HazelcastThreadGroup(
                "hz",
                loggingService.getLogger(HazelcastThreadGroup.class),
                getClass().getClassLoader());

        serverSocketChannel = ServerSocketChannel.open();
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.setReuseAddress(true);
        serverSocket.setSoTimeout(1000);
        serverSocket.bind(new InetSocketAddress("0.0.0.0", port));
        thisAddress = new Address("127.0.0.1", port);
        this.serializationService = (InternalSerializationService) new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    @Override
    public void onOutOfMemory(OutOfMemoryError oom) {

    }

    @Override
    public Address getThisAddress() {
        return thisAddress;
    }

    @Override
    public void onFatalError(Exception e) {

    }

    @Override
    public SocketInterceptorConfig getSocketInterceptorConfig() {
        return null;
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
    public void handleClientMessage(ClientMessage cm, Connection connection) {

    }

    @Override
    public TextCommandService getTextCommandService() {
        return null;
    }

    @Override
    public boolean isMemcacheEnabled() {
        return false;
    }

    @Override
    public boolean isRestEnabled() {
        return false;
    }

    @Override
    public void removeEndpoint(Address endpoint) {

    }

    @Override
    public String getThreadPrefix() {
        return hazelcastThreadGroup.getThreadPoolNamePrefix("IO");
    }

    @Override
    public ThreadGroup getThreadGroup() {
        return hazelcastThreadGroup.getInternalThreadGroup();
    }

    @Override
    public void onSuccessfulConnection(Address address) {

    }

    @Override
    public void onFailedConnection(Address address) {

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
    public int getSocketReceiveBufferSize() {
        return 32;
    }

    @Override
    public int getSocketSendBufferSize() {
        return 32;
    }

    @Override
    public int getSocketClientReceiveBufferSize() {
        return 32;
    }

    @Override
    public int getSocketClientSendBufferSize() {
        return 32;
    }

    @Override
    public boolean isSocketBufferDirect() {
        return false;
    }

    @Override
    public int getSocketLingerSeconds() {
        return 0;
    }

    @Override
    public int getSocketConnectTimeoutSeconds() {
        return 0;
    }

    @Override
    public boolean getSocketKeepAlive() {
        return true;
    }

    @Override
    public boolean getSocketNoDelay() {
        return true;
    }

    @Override
    public int getInputSelectorThreadCount() {
        return 1;
    }

    @Override
    public int getOutputSelectorThreadCount() {
        return 1;
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
    public int getBalancerIntervalSeconds() {
        return 0;
    }

    @Override
    public void onDisconnect(Address endpoint, Throwable cause) {

    }

    @Override
    public boolean isClient() {
        return false;
    }

    @Override
    public void executeAsync(final Runnable runnable) {
        new Thread() {
            public void run() {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    loggingService.getLogger(MockIOService.class).severe(t);
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
    public Data toData(Object obj) {
        return serializationService.toData(obj);
    }

    @Override
    public Object toObject(Data data) {
        return serializationService.toObject(data);
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        return new DefaultSocketChannelWrapperFactory();
    }

    @Override
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        return null;
    }

    @Override
    public ReadHandler createReadHandler(final TcpIpConnection connection) {
        return new MemberReadHandler(connection, new PacketDispatcher() {
            private ILogger logger = getLogger("MockIOService");

            @Override
            public void dispatch(Packet packet) {
                try {
                    if (packet.isFlagSet(Packet.FLAG_BIND)) {
                        connection.getConnectionManager().handle(packet);
                    } else {
                        PacketHandler handler = packetHandler;
                        if (handler != null) {
                            handler.handle(packet);
                        }
                    }
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
        });
    }

    @Override
    public WriteHandler createWriteHandler(TcpIpConnection connection) {
        return new MemberWriteHandler();
    }

}
