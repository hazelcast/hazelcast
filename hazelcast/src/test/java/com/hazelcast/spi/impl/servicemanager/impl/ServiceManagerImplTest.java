package com.hazelcast.spi.impl.servicemanager.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.ConfigurableService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.swing.*;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ServiceManagerImplTest extends HazelcastTestSupport {

    private ServiceManagerImpl serviceManager;

    @Before
    public void setup() {
        Config config = new Config();

        ServiceConfig serviceConfig = new ServiceConfig()
                .setClassName(FooService.class.getName())
                .setEnabled(true)
                .setName("fooService");
        config.getServicesConfig().addServiceConfig(serviceConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        serviceManager = new ServiceManagerImpl(nodeEngine);
        serviceManager.start();
    }

    static class FooService implements ManagedService, ConfigurableService{
        volatile boolean initCalled;
        volatile boolean configureCalled;

        @Override
        public void configure(Object configObject) {
            this.configureCalled = true;
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            this.initCalled = true;
        }

        @Override
        public void reset() {

        }

        @Override
        public void shutdown(boolean terminate) {

        }
    }

    // ===================== getServiceInfo ================================


    @Test
    public void getServiceInfo() {
        ServiceInfo result = serviceManager.getServiceInfo(MapService.SERVICE_NAME);
        assertNotNull(result);
        assertEquals(MapService.SERVICE_NAME, result.getName());
        assertInstanceOf(MapService.class, result.getService());
    }

    @Test
    public void getServiceInfo_notExisting() {
        ServiceInfo result = serviceManager.getServiceInfo("notexisting");
        assertNull(result);
    }

    // ===================== getServiceInfos ================================

    @Test
    public void getServiceInfos() {
        List<ServiceInfo> result = serviceManager.getServiceInfos(MapService.class);
        assertNotNull(result);
        assertEquals(1, result.size());

        ServiceInfo serviceInfo = result.get(0);
        assertEquals(MapService.SERVICE_NAME, serviceInfo.getName());
        assertInstanceOf(MapService.class, serviceInfo.getService());
    }

    @Test
    public void getServiceInfos_notExisting() {
        List<ServiceInfo> result = serviceManager.getServiceInfos(JPanel.class);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    // ==================== getService =====================================

    @Test
    public void getService() {
        Object result = serviceManager.getService(MapService.SERVICE_NAME);
        assertInstanceOf(MapService.class, result);
    }

    @Test
    public void getService_notExisting() {
        Object result = serviceManager.getService("notexisting");
        assertNull(result);
    }

    // ======================== getSharedService ============================

    @Test
    public void getSharedService() {
        Object result = serviceManager.getSharedService(LockService.SERVICE_NAME);
        assertInstanceOf(LockService.class, result);
    }

    @Test
    public void getSharedService_notExisting() {
        Object result = serviceManager.getSharedService("notexisting");
        assertNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getSharedService_notSharedService() {
        serviceManager.getSharedService(MapService.SERVICE_NAME);
    }

    // ========================= getServices =====================================

    @Test
    public void getServices() {
        List<MapService> result = serviceManager.getServices(MapService.class);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertInstanceOf(MapService.class, result.get(0));
    }

    @Test
    public void getServices_notExisting() {
        List<JPanel> result = serviceManager.getServices(JPanel.class);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ========================= userService =====================================

    @Test
    public void userService() {
        Object result = serviceManager.getService("fooService");
        FooService fooService = assertInstanceOf(FooService.class, result);

        assertTrue(fooService.initCalled);
        assertTrue(fooService.configureCalled);
    }
}
