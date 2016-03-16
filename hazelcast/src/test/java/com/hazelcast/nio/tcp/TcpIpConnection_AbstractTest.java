package com.hazelcast.nio.tcp;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.tcp.nonblocking.Select_NonBlockingIOThreadingModelFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class TcpIpConnection_AbstractTest extends HazelcastTestSupport {

    protected IOThreadingModelFactory threadingModelFactory = new Select_NonBlockingIOThreadingModelFactory();

    protected ILogger logger;
    protected LoggingServiceImpl loggingService;
    protected InternalSerializationService serializationService;

    protected Address addressA;
    protected TcpIpConnectionManager connManagerA;
    protected MockIOService ioServiceA;
    protected MetricsRegistryImpl metricsRegistryA;

    protected Address addressB;
    protected TcpIpConnectionManager connManagerB;
    protected MockIOService ioServiceB;
    protected MetricsRegistryImpl metricsRegistryB;

    protected TcpIpConnectionManager connManagerC;
    protected Address addressC;
    protected MockIOService ioServiceC;
    protected MetricsRegistryImpl metricsRegistryC;

    @Before
    public void setup() throws Exception {
        addressA = new Address("127.0.0.1", 5701);
        addressB = new Address("127.0.0.1", 5702);
        addressC = new Address("127.0.0.1", 5703);

        loggingService = new LoggingServiceImpl("somegroup", "log4j", BuildInfoProvider.getBuildInfo());
        logger = loggingService.getLogger(TcpIpConnection_AbstractTest.class);

        metricsRegistryA = newMetricsRegistry();
        connManagerA = newConnectionManager(addressA.getPort(), metricsRegistryA);
        ioServiceA = (MockIOService) connManagerA.getIoService();

        metricsRegistryB = newMetricsRegistry();
        connManagerB = newConnectionManager(addressB.getPort(), metricsRegistryB);
        ioServiceB = (MockIOService) connManagerB.getIoService();

        metricsRegistryC = newMetricsRegistry();
        connManagerC = newConnectionManager(addressC.getPort(), metricsRegistryC);
        ioServiceC = (MockIOService) connManagerB.getIoService();

        serializationService = (InternalSerializationService) new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    public void startAllConnectionManagers() {
        connManagerA.start();
        connManagerB.start();
        connManagerC.start();
    }

    @After
    public void tearDown() {
        connManagerA.shutdown();
        connManagerB.shutdown();
        connManagerC.shutdown();

        metricsRegistryA.shutdown();
        metricsRegistryB.shutdown();
        metricsRegistryC.shutdown();
    }

    protected MetricsRegistryImpl newMetricsRegistry() {
        return new MetricsRegistryImpl(loggingService.getLogger(MetricsRegistryImpl.class), INFO);
    }

    protected TcpIpConnectionManager newConnectionManager(int port, MetricsRegistry metricsRegistry) throws Exception {
        MockIOService ioService = new MockIOService(port);

        return new TcpIpConnectionManager(
                ioService,
                ioService.serverSocketChannel,
                ioService.loggingService,
                metricsRegistry,
                threadingModelFactory.create(ioService, metricsRegistry));
    }

    // ====================== support ========================================

    protected TcpIpConnection connect(Address address) {
        return connect(connManagerA, address);
    }

    protected TcpIpConnection connect(final TcpIpConnectionManager connectionManager, final Address address) {
        connectionManager.getOrConnect(address);

        final AtomicReference<TcpIpConnection> ref = new AtomicReference<TcpIpConnection>();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Connection c = connectionManager.getConnection(address);
                assertNotNull(c);
                ref.set((TcpIpConnection) c);
            }
        });

        return ref.get();
    }

    public static TcpIpConnection getConnection(TcpIpConnectionManager connManager, SocketAddress localSocketAddress) {
        long startMs = System.currentTimeMillis();

        for (; ; ) {
            for (TcpIpConnection connection : connManager.getActiveConnections()) {
                if (connection.getRemoteSocketAddress().equals(localSocketAddress)) {
                    return connection;
                }
            }

            if (startMs + 20000 < System.currentTimeMillis()) {
                fail("Timeout: Could not find connection");
            }

            sleepMillis(100);
        }
    }
}
