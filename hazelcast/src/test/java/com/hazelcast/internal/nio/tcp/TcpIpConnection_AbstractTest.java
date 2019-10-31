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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.internal.networking.nio.Select_NioNetworkingFactory;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@SuppressWarnings("WeakerAccess")
public abstract class TcpIpConnection_AbstractTest extends HazelcastTestSupport {

    private static final int PORT_NUMBER_UPPER_LIMIT = 5799;
    private int portNumber = 5701;

    protected NetworkingFactory networkingFactory = new Select_NioNetworkingFactory();

    protected ILogger logger;
    protected LoggingServiceImpl loggingService;
    protected InternalSerializationService serializationService;

    protected Address addressA;
    protected Address addressB;
    protected Address addressC;

    protected TcpIpNetworkingService networkingServiceA;
    protected TcpIpNetworkingService networkingServiceB;
    protected TcpIpNetworkingService networkingServiceC;

    protected MockIOService ioServiceA;
    protected MockIOService ioServiceB;
    protected MockIOService ioServiceC;

    protected MetricsRegistryImpl metricsRegistryA;
    protected MetricsRegistryImpl metricsRegistryB;
    protected MetricsRegistryImpl metricsRegistryC;

    @Before
    public void setup() throws Exception {

        loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo());
        logger = loggingService.getLogger(TcpIpConnection_AbstractTest.class);

        metricsRegistryA = newMetricsRegistry();
        networkingServiceA = newNetworkingService(metricsRegistryA);
        ioServiceA = (MockIOService) networkingServiceA.getIoService();
        addressA = ioServiceA.getThisAddress();

        metricsRegistryB = newMetricsRegistry();
        networkingServiceB = newNetworkingService(metricsRegistryB);
        ioServiceB = (MockIOService) networkingServiceB.getIoService();
        addressB = ioServiceB.getThisAddress();

        metricsRegistryC = newMetricsRegistry();
        networkingServiceC = newNetworkingService(metricsRegistryC);
        ioServiceC = (MockIOService) networkingServiceC.getIoService();
        addressC = ioServiceC.getThisAddress();

        serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    @After
    public void tearDown() {
        networkingServiceA.shutdown();
        networkingServiceB.shutdown();
        networkingServiceC.shutdown();

        metricsRegistryA.shutdown();
        metricsRegistryB.shutdown();
        metricsRegistryC.shutdown();
    }

    protected void startAllNetworkingServices() {
        networkingServiceA.start();
        networkingServiceB.start();
        networkingServiceC.start();
    }

    protected MetricsRegistryImpl newMetricsRegistry() {
        return new MetricsRegistryImpl(loggingService.getLogger(MetricsRegistryImpl.class), INFO);
    }

    protected TcpIpNetworkingService newNetworkingService(MetricsRegistry metricsRegistry) throws Exception {
        MockIOService ioService = null;
        while (ioService == null) {
            try {
                ioService = new MockIOService(portNumber++);
            } catch (IOException e) {
                if (portNumber >= PORT_NUMBER_UPPER_LIMIT) {
                    throw e;
                }
            }
        }

        ServerSocketRegistry registry = new ServerSocketRegistry(singletonMap(MEMBER, ioService.serverSocketChannel), true);

        final MockIOService finalIoService = ioService;
        TcpIpNetworkingService tcpIpNetworkingService = new TcpIpNetworkingService(null,
                ioService,
                registry,
                ioService.loggingService,
                metricsRegistry,
                networkingFactory.create(ioService, metricsRegistry),
                qualifier -> new UnifiedChannelInitializer(finalIoService));
        return tcpIpNetworkingService;
    }

    // ====================== support ========================================

    protected TcpIpConnection connect(Address address) {
        return connect(networkingServiceA, address);
    }

    protected TcpIpConnection connect(final TcpIpNetworkingService service, final Address address) {
        service.getEndpointManager(MEMBER).getOrConnect(address);

        final AtomicReference<TcpIpConnection> ref = new AtomicReference<>();
        assertTrueEventually(() -> {
            Connection c = service.getEndpointManager(MEMBER).getConnection(address);
            assertNotNull(c);
            ref.set((TcpIpConnection) c);
        });

        return ref.get();
    }

    public static TcpIpConnection getConnection(TcpIpNetworkingService service, SocketAddress localSocketAddress) {
        long startMs = System.currentTimeMillis();

        for (; ; ) {
            for (TcpIpConnection connection : service.getEndpointManager(MEMBER).getActiveConnections()) {
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
