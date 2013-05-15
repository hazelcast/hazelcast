package com.hazelcast.client;

import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.nio.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @mdogan 5/14/13
 */
@RunWith(RandomBlockJUnit4ClassRunner.class)
public abstract class ClientTestSupport {

    private TestClient client;

    @Before
    public final void startClient() throws IOException {
        int port = 5701;
        if (StaticNodeFactory.MOCK_NETWORK){
            port = 5001;
        }
        client = StaticNodeFactory.newClient(new Address("127.0.0.1",port));
        client.auth();
    }

    @After
    public final void closeClient() throws IOException {
        client.close();
    }

    protected final TestClient client() {
        return client;
    }


}
