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

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpServerConnectionManager_ConnectionListenerTest
        extends TcpServerConnection_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void addConnectionListener_whenNull() {
        tcpServerA.getConnectionManager(MEMBER).addConnectionListener(null);
    }

    @Test
    public void whenConnectionAdded() {
        startAllTcpServers();

        ConnectionListener listener = mock(ConnectionListener.class);
        tcpServerA.getConnectionManager(MEMBER).addConnectionListener(listener);

        Connection c = connect(tcpServerA, addressB);

        assertTrueEventually(() -> verify(listener).connectionAdded(c));
    }

    @Test
    public void whenConnectionDestroyed() {
        startAllTcpServers();

        ConnectionListener listener = mock(ConnectionListener.class);
        tcpServerA.getConnectionManager(MEMBER).addConnectionListener(listener);

        Connection c = connect(tcpServerA, addressB);
        c.close(null, null);

        assertTrueEventually(() -> verify(listener).connectionRemoved(c));
    }

    @Test
    public void whenConnectionManagerShutdown_thenListenersRemoved() {
        startAllTcpServers();

        ConnectionListener listener = mock(ConnectionListener.class);
        tcpServerA.getConnectionManager(MEMBER).addConnectionListener(listener);

        tcpServerA.shutdown();

        TcpServerConnectionManager connectionManager = tcpServerA.getConnectionManager(EndpointQualifier.MEMBER);

        assertEquals(0, connectionManager.connectionListeners.size());
    }
}
