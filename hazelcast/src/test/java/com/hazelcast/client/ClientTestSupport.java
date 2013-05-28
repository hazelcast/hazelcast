package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import com.hazelcast.test.StaticNodeFactory;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @mdogan 5/14/13
 */
@RunWith(RandomBlockJUnit4ClassRunner.class)
public abstract class ClientTestSupport {

    @Rule
    public final ClientTestResource clientResource = new ClientTestResource(createConfig());

    protected final HazelcastInstance getInstance() {
        return clientResource.instance;
    }

    protected final SimpleClient getClient() {
        return clientResource.client;
    }

    protected abstract Config createConfig();


    public static final class ClientTestResource extends ExternalResource {
        private final Config config;
        private HazelcastInstance instance;
        private SimpleClient client;

        public ClientTestResource(Config config) {
            this.config = config;
        }

        protected void before() throws Throwable {
            instance = new StaticNodeFactory(1).newHazelcastInstance(config);
            final Address address = TestUtil.getNode(instance).getThisAddress();
            client = StaticNodeFactory.newClient(address);
            client.auth();
        }

        protected void after() {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            instance.getLifecycleService().shutdown();
        }
    }
}
