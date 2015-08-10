package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_ConnectionListenerTest extends TcpIpConnection_AbstractTest {

    @Test(expected = NullPointerException.class)
    public void addConnectionListener_whenNull() {
        connManagerA.addConnectionListener(null);
    }

    @Test
    public void whenConnectionAdded() throws Exception {
        startAllConnectionManagers();

        final ConnectionListener listener = mock(ConnectionListener.class);
        connManagerA.addConnectionListener(listener);

        final Connection c = connect(connManagerA, addressB);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                listener.connectionAdded(c);
            }
        });
    }

    @Test
    public void whenConnectionDestroyed() throws Exception {
        startAllConnectionManagers();


        final ConnectionListener listener = mock(ConnectionListener.class);
        connManagerA.addConnectionListener(listener);

        final Connection c = connect(connManagerA, addressB);
        connManagerA.destroyConnection(c);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                listener.connectionRemoved(c);
            }
        });
    }

    @Test
    public void whenConnectionManagerShutdown_thenListenersRemoved() {
        startAllConnectionManagers();

        ConnectionListener listener = mock(ConnectionListener.class);
        connManagerA.addConnectionListener(listener);

        connManagerA.shutdown();

        assertEquals(0, connManagerA.connectionListeners.size());
    }
}
