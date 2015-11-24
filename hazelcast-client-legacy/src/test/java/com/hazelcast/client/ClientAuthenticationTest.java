package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientAuthenticationTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    @Test(expected = IllegalStateException.class)
    public void testFailedAuthentication() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1);
        clientConfig.getGroupConfig().setPassword("InvalidPassword");
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testNoClusterFound() throws Exception {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptPeriod(1);
        hazelcastFactory.newHazelcastClient(clientConfig);

    }

    @Test
    public void testAuthenticationWithCustomCredentials() {
        final String username = "dev";
        final String password = "pass";

        PortableFactory credentialsFactory = new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new CustomCredentials() {
                    @Override
                    public String getUsername() {
                        return username;
                    }
                    @Override
                    public String getPassword() {
                        return password;
                    }
                };
            }
        };

        // with this config, the server will authenticate any credential of type CustomCredentials
        Config config = new Config();
        config.getGroupConfig()
                .setName(username)
                .setPassword(password);
        config.getSerializationConfig()
                .addPortableFactory(1, credentialsFactory);
        hazelcastFactory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();

        // make sure there are no credentials sent over the wire
        clientConfig.getSecurityConfig().setCredentials(new CustomCredentials());
        hazelcastFactory.newHazelcastClient(clientConfig);
    }

    private class CustomCredentials extends UsernamePasswordCredentials {

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }
    }
}
