package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.ClientResponseHandlerSupplier.AsyncMultiThreadedResponseHandler;
import com.hazelcast.client.spi.impl.ClientResponseHandlerSupplier.AsyncSingleThreadedResponseHandler;
import com.hazelcast.client.spi.impl.ClientResponseHandlerSupplier.SyncResponseHandler;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.spi.properties.ClientProperty.RESPONSE_THREAD_COUNT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientResponseHandlerSupplierTest extends ClientTestSupport {
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void before() {
        hazelcastFactory.newHazelcastInstance();
    }

    @After
    public void after() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNegativeResponseThreads() {
        getResponseHandler(-1);
    }

    @Test
    public void whenZeroResponseThreads() {
        ClientResponseHandler handler = getResponseHandler(0);
        assertInstanceOf(SyncResponseHandler.class, handler);
    }

    @Test
    public void whenOneResponseThreads() {
        ClientResponseHandler handler = getResponseHandler(1);
        assertInstanceOf(AsyncSingleThreadedResponseHandler.class, handler);
    }

    @Test
    public void whenMultipleResponseThreads() {
        ClientResponseHandler handler = getResponseHandler(2);
        assertInstanceOf(AsyncMultiThreadedResponseHandler.class, handler);
    }

    private ClientResponseHandler getResponseHandler(int threadCount) {
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(
                new ClientConfig()
                        .setProperty(RESPONSE_THREAD_COUNT.getName(), "" + threadCount));
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        AbstractClientInvocationService invocationService = (AbstractClientInvocationService) clientInstanceImpl.getInvocationService();

        ClientResponseHandlerSupplier responseHandlerSupplier = new ClientResponseHandlerSupplier(invocationService);
        return responseHandlerSupplier.get();
    }
}
