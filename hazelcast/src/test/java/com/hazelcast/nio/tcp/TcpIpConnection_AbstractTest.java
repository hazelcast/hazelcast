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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.networking.nio.Select_NioNetworkingFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

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

    protected TcpIpConnectionManager connManagerA;
    protected TcpIpConnectionManager connManagerB;
    protected TcpIpConnectionManager connManagerC;

    protected MockIOService ioServiceA;
    protected MockIOService ioServiceB;
    protected MockIOService ioServiceC;

    protected MetricsRegistry metricsRegistryA;
    protected MetricsRegistry metricsRegistryB;
    protected MetricsRegistry metricsRegistryC;

    @Before
    public void setup() throws Exception {

        loggingService = new LoggingServiceImpl("somegroup", "log4j2", BuildInfoProvider.getBuildInfo());
        logger = loggingService.getLogger(TcpIpConnection_AbstractTest.class);

        connManagerA = newConnectionManager();
        ioServiceA = (MockIOService) connManagerA.getIoService();
        addressA = ioServiceA.getThisAddress();

        connManagerB = newConnectionManager();
        ioServiceB = (MockIOService) connManagerB.getIoService();
        addressB = ioServiceB.getThisAddress();

        connManagerC = newConnectionManager();
        ioServiceC = (MockIOService) connManagerC.getIoService();
        addressC = ioServiceC.getThisAddress();

        serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(TestDataFactory.FACTORY_ID, new TestDataFactory())
                .build();
    }

    @After
    public void tearDown() {
        connManagerA.shutdown();
        connManagerB.shutdown();
        connManagerC.shutdown();
    }

    protected void startAllConnectionManagers() {
        connManagerA.start();
        connManagerB.start();
        connManagerC.start();
    }

    protected MetricsRegistry newMetricsRegistry() {
        return new MetricsRegistryImpl();
    }

    protected TcpIpConnectionManager newConnectionManager() throws Exception {
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

        return new TcpIpConnectionManager(
                ioService,
                ioService.serverSocketChannel,
                ioService.loggingService,
                networkingFactory.create(ioService));
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
            public void run() {
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
