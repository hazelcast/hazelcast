package com.hazelcast.nio.tcp;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

public abstract class TcpIpConnection_AbstractTest extends HazelcastTestSupport {

    protected ILogger logger;
    protected LoggingServiceImpl loggingService;
    protected SerializationService serializationService;

    protected Address addressA;
    protected TcpIpConnectionManager connManagerA;
    protected MockIOService ioServiceA;

    protected Address addressB;
    protected TcpIpConnectionManager connManagerB;
    protected MockIOService ioServiceB;

    protected TcpIpConnectionManager connManagerC;
    protected Address addressC;
    protected MockIOService ioServiceC;

    @Before
    public void setup() throws Exception {
        addressA = new Address("127.0.0.1", 5701);
        addressB = new Address("127.0.0.1", 5702);
        addressC = new Address("127.0.0.1", 5703);

        loggingService = new LoggingServiceImpl("somegroup", "log4j", BuildInfoProvider.getBuildInfo());
        logger = loggingService.getLogger(TcpIpConnection_AbstractTest.class);

        connManagerA = newConnectionManager(addressA.getPort());
        ioServiceA = (MockIOService) connManagerA.getIOHandler();

        connManagerB = newConnectionManager(addressB.getPort());
        ioServiceB = (MockIOService) connManagerB.getIOHandler();

        connManagerC = newConnectionManager(addressC.getPort());
        ioServiceC = (MockIOService) connManagerB.getIOHandler();

        serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    public void startAllConnectionManagers(){
        connManagerA.start();
        connManagerB.start();
        connManagerC.start();
    }

    @After
    public void tearDown() {
        connManagerA.shutdown();
        connManagerB.shutdown();
        connManagerC.shutdown();
    }

    protected TcpIpConnectionManager newConnectionManager(int port) throws Exception {
        MockIOService ioService = new MockIOService(port);

        return new TcpIpConnectionManager(
                ioService,
                ioService.serverSocketChannel,
                ioService.hazelcastThreadGroup,
                ioService.loggingService);
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
}
