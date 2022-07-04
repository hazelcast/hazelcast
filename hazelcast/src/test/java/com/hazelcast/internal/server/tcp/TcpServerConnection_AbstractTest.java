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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.networking.nio.Select_NioNetworkingFactory;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.server.MockServerContext;
import com.hazelcast.internal.server.NetworkingFactory;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.TestDataFactory;
import com.hazelcast.internal.util.UuidUtil;
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
public abstract class TcpServerConnection_AbstractTest extends HazelcastTestSupport {

    private static final int PORT_NUMBER_UPPER_LIMIT = 5799;
    private int portNumber = 5701;

    protected NetworkingFactory networkingFactory = new Select_NioNetworkingFactory();

    protected ILogger logger;
    protected LoggingServiceImpl loggingService;
    protected InternalSerializationService serializationService;

    protected Address addressA;
    protected Address addressB;
    protected Address addressC;

    protected TcpServer tcpServerA;
    protected TcpServer tcpServerB;
    protected TcpServer tcpServerC;

    protected MockServerContext serverContextA;
    protected MockServerContext serverContextB;
    protected MockServerContext serverContextC;

    protected MetricsRegistryImpl metricsRegistryA;
    protected MetricsRegistryImpl metricsRegistryB;
    protected MetricsRegistryImpl metricsRegistryC;

    @Before
    public void setup() throws Exception {
        loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo(), true, null);
        logger = loggingService.getLogger(TcpServerConnection_AbstractTest.class);

        metricsRegistryA = newMetricsRegistry();

        tcpServerA = newMockTcpServer(metricsRegistryA);
        serverContextA = (MockServerContext) tcpServerA.getContext();
        addressA = serverContextA.getThisAddress();

        metricsRegistryB = newMetricsRegistry();
        tcpServerB = newMockTcpServer(metricsRegistryB);
        serverContextB = (MockServerContext) tcpServerB.getContext();
        addressB = serverContextB.getThisAddress();

        metricsRegistryC = newMetricsRegistry();
        tcpServerC = newMockTcpServer(metricsRegistryC);
        serverContextC = (MockServerContext) tcpServerC.getContext();
        addressC = serverContextC.getThisAddress();

        serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    @After
    public void tearDown() {
        tcpServerA.shutdown();
        tcpServerB.shutdown();
        tcpServerC.shutdown();

        metricsRegistryA.shutdown();
        metricsRegistryB.shutdown();
        metricsRegistryC.shutdown();
    }

    protected void startAllTcpServers() {
        tcpServerA.start();
        tcpServerB.start();
        tcpServerC.start();
    }

    protected MetricsRegistryImpl newMetricsRegistry() {
        return new MetricsRegistryImpl(loggingService.getLogger(MetricsRegistryImpl.class), INFO);
    }

    protected TcpServer newMockTcpServer(MetricsRegistry metricsRegistry) throws Exception {
        MockServerContext serverContext = null;
        while (serverContext == null) {
            try {
                serverContext = new MockServerContext(portNumber++, UuidUtil.newUnsecureUUID());
            } catch (IOException e) {
                if (portNumber >= PORT_NUMBER_UPPER_LIMIT) {
                    throw e;
                }
            }
        }

        ServerSocketRegistry registry = new ServerSocketRegistry(singletonMap(MEMBER, serverContext.serverSocketChannel), true);
        LocalAddressRegistry addressRegistry = new LocalAddressRegistry(logger);
        MockServerContext finalServiceContext = serverContext;
        return new TcpServer(null,
                serverContext,
                registry,
                addressRegistry,
                metricsRegistry,
                networkingFactory.create(serverContext, metricsRegistry),
                qualifier -> new UnifiedChannelInitializer(finalServiceContext));
    }

    // ====================== support ========================================

    protected TcpServerConnection connect(Address address) {
        return connect(tcpServerA, address);
    }

    protected TcpServerConnection connect(final TcpServer service, final Address address) {
        service.getConnectionManager(MEMBER).getOrConnect(address);

        final AtomicReference<TcpServerConnection> ref = new AtomicReference<>();
        assertTrueEventually(() -> {
            Connection c = service.getConnectionManager(MEMBER).get(address);
            assertNotNull(c);
            ref.set((TcpServerConnection) c);
        });

        return ref.get();
    }

    public static ServerConnection getConnection(TcpServer server, SocketAddress localSocketAddress) {
        long startMs = System.currentTimeMillis();

        for (; ; ) {
            for (ServerConnection connection : server.getConnectionManager(MEMBER).getConnections()) {
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
