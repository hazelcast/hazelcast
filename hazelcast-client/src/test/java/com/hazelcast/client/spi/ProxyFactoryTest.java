package com.hazelcast.client.spi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProxyFactoryTest {

    static final String SERVICE_NAME = "CustomService";

    static HazelcastInstance server;

    @BeforeClass
    public static void startServer() {
        Config config = new Config();
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true).setName(SERVICE_NAME)
                .setServiceImpl(new CustomService());
        config.getServicesConfig().addServiceConfig(serviceConfig);

        server = Hazelcast.newHazelcastInstance(config);
    }

    @AfterClass
    public static void shutdownServer() {
        Hazelcast.shutdownAll();
    }

    @After
    public void shutdownClient() {
        HazelcastClient.shutdownAll();
    }

    @Test
    public void testCustomProxy_usingFactoryClassName() {
        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setService(SERVICE_NAME);
        proxyFactoryConfig.setClassName(CustomProxyFactory.class.getName());
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);

        testCustomProxy(clientConfig);
    }

    @Test
    public void testCustomProxy_usingFactoryImplementation() {
        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setService(SERVICE_NAME);
        proxyFactoryConfig.setFactoryImpl(new CustomProxyFactory());
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);

        testCustomProxy(clientConfig);
    }

    private void testCustomProxy(ClientConfig clientConfig) {
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        String objectName = "custom-object";
        CustomClientProxy proxy = client.getDistributedObject(SERVICE_NAME, objectName);
        Assert.assertEquals(SERVICE_NAME, proxy.getServiceName());
        Assert.assertEquals(objectName, proxy.getName());
    }

    private static class CustomService implements RemoteService {
        @Override
        public DistributedObject createDistributedObject(String objectName) {
            return new CustomClientProxy(SERVICE_NAME, objectName);
        }

        @Override
        public void destroyDistributedObject(String objectName) {
        }
    }

    private static class CustomProxyFactory implements ClientProxyFactory {

        @Override
        public ClientProxy create(String id) {
            return new CustomClientProxy(SERVICE_NAME, id);
        }
    }

    private static class CustomClientProxy extends ClientProxy {

        protected CustomClientProxy(String serviceName, String objectName) {
            super(serviceName, objectName);
        }

        @Override
        protected void onDestroy() {
        }
    }

}
