package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @mdogan 5/14/13
 */
@RunWith(RandomBlockJUnit4ClassRunner.class)
public abstract class ClientTestSupport {

    private HazelcastInstance instance;
    private SimpleClient client;

    @BeforeClass
    @AfterClass
    public static void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Before
    public final void init() throws IOException {
        instance = new StaticNodeFactory(1).newHazelcastInstance(createConfig());
        final Address address = TestUtil.getNode(instance).getThisAddress();
        client = StaticNodeFactory.newClient(address);
        client.auth();
    }

    @After
    public final void destroy() throws IOException {
        client.close();
        instance.getLifecycleService().shutdown();
    }

    protected final HazelcastInstance getInstance() {
        return instance;
    }

    protected final SimpleClient getClient() {
        return client;
    }

    protected abstract Config createConfig();

}
