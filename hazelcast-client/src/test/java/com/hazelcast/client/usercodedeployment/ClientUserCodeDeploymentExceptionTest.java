package com.hazelcast.client.usercodedeployment;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import java.io.FileNotFoundException;

import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientUserCodeDeploymentExceptionTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void testUserCodeDeploymentIsDisabledByDefaultOnClient() {
        // this test also validate the EP is filtered locally and has to be loaded from the other member
        ClientConfig clientConfig = new ClientConfig();
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap(randomName());
        try {
            map.executeOnEntries(incrementingEntryProcessor);
            assertTrue(false);
        } catch (HazelcastSerializationException e) {
            assertEquals(ClassNotFoundException.class, e.getCause().getClass());
        }
    }

    private Config createNodeConfig() {
        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        return i2Config;
    }


    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        return config;
    }

    @Test(expected = IllegalStateException.class)
    public void testUserCodeDeployment_serverIsNotEnabled() {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getUserCodeDeploymentConfig().setEnabled(true);
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        factory.newHazelcastClient(clientConfig);
    }

    @Test(expected = IllegalStateException.class)
    public void testClientsWithConflictingClassRepresentations() {
        /*
            These two jars IncrementingEntryProcessor.jar and IncrementingEntryProcessorConflicting.jar
            contains same class `IncrementingEntryProcessor` with different implementations
         */
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHazelcastInstance(config);

        {
            ClientConfig clientConfig = new ClientConfig();
            ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
            clientUserCodeDeploymentConfig.addJar("IncrementingEntryProcessor.jar").setEnabled(true);
            clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
            factory.newHazelcastClient(clientConfig);
        }

        {
            ClientConfig clientConfig = new ClientConfig();
            ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
            clientUserCodeDeploymentConfig.addJar("IncrementingEntryProcessorConflicting.jar").setEnabled(true);
            clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
            factory.newHazelcastClient(clientConfig);
        }

    }

    @Test(expected = ClassNotFoundException.class)
    public void testClientsWith_wrongClassName() throws Throwable {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("NonExisting.class").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        try {
            factory.newHazelcastClient(clientConfig);
        } catch (HazelcastException e) {
            throw e.getCause();
        }

    }

    @Test(expected = FileNotFoundException.class)
    public void testClientsWith_wrongJarPath() throws Throwable {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addJar("NonExisting.jar").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        try {
            factory.newHazelcastClient(clientConfig);
        } catch (HazelcastException e) {
            throw e.getCause();
        }

    }

}
