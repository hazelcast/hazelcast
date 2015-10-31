package com.hazelcast.osgi;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.osgi.impl.HazelcastInternalOSGiService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.osgi.framework.BundleException;
import org.osgi.framework.ServiceReference;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastOSGiServiceTest {

    private TestBundle bundle;
    private TestBundleContext bundleContext;

    @Before
    public void setup() throws BundleException {
        bundle = new TestBundle();
        bundleContext = bundle.getBundleContext();
        bundle.start();
    }

    @After
    public void tearDown() throws BundleException {
        try  {
            bundle.stop();
            bundle = null;
            bundleContext = null;
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private HazelcastInternalOSGiService getService(TestBundleContext bundleContext) {
        ServiceReference serviceRef =
                bundleContext.getServiceReference(HazelcastOSGiService.class.getName());
        if (serviceRef == null) {
            return null;
        }
        return (HazelcastInternalOSGiService) bundleContext.getService(serviceRef);
    }

    private HazelcastInternalOSGiService getService() {
        return getService(bundleContext);
    }

    @Test
    public void serviceRetrievedSuccessfully() {
        HazelcastInternalOSGiService service = getService();

        assertNotNull(service);
    }

    @Test
    public void bundleOfServiceRetrievedSuccessfully() {
        HazelcastInternalOSGiService service = getService();

        assertEquals(bundle, service.getOwnerBundle());
    }

    @Test
    public void idOfServiceRetrievedSuccessfully() {
        HazelcastInternalOSGiService service = getService();

        assertEquals(HazelcastInternalOSGiService.DEFAULT_ID, service.getId());
    }

    @Test
    public void serviceDeactivatedAndThenActivatedSuccessfully() throws BundleException {
        HazelcastInternalOSGiService service = getService();

        assertTrue(service.isActive());

        bundle.stop();

        assertFalse(service.isActive());
        assertNull(getService());

        bundle.start();

        assertTrue(service.isActive());
        assertNotNull(getService());
    }

    @Test
    public void defaultInstanceNotExistWhenItIsNotSpecified() {
        HazelcastInternalOSGiService service = getService();

        assertNull(service.getDefaultHazelcastInstance());
    }

    @Test
    public void defaultInstanceExistWhenItIsSpecified() throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_START);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_START, "true");

            testBundle = new TestBundle();
            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundle.getBundleContext());

            assertNotNull(service.getDefaultHazelcastInstance());
        } finally {
            if (propValue != null) {
                System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_START, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void newInstanceRetrievedSuccessfullyWithoutConfiguration() {
        HazelcastInternalOSGiService service = getService();

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance();
        assertNotNull(osgiInstance);

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
    }

    @Test
    public void newInstanceRetrievedSuccessfullyWithConfiguration() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HazelcastInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertNotNull(osgiInstance);
        assertEquals(config, osgiInstance.getConfig());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(config, instance.getConfig());
    }

    @Test
    public void newInstanceRegisteredAsServiceWhenRegistrationIsNotDisabled()
            throws BundleException {
        HazelcastInternalOSGiService service = getService();

        service.newHazelcastInstance();

        assertNull(bundleContext.getServiceReference(HazelcastOSGiInstance.class.getName()));
    }

    @Test
    public void newInstanceNotRegisteredAsServiceWhenRegistrationIsDisabled()
            throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_REGISTER_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_REGISTER_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);

            service.newHazelcastInstance();

            assertNull(testBundleContext.getServiceReference(HazelcastOSGiInstance.class.getName()));
        } finally {
            if (propValue != null) {
                System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_REGISTER_DISABLED, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void groupNameIsSetToDefaultGroupNameOfBundleWhenGroupingIsNotDisabled() {
        HazelcastInternalOSGiService service = getService();

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance();
        assertNotNull(osgiInstance);
        assertEquals(HazelcastInternalOSGiService.DEFAULT_GROUP_NAME,
                     osgiInstance.getConfig().getGroupConfig().getName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(HazelcastInternalOSGiService.DEFAULT_GROUP_NAME,
                     instance.getConfig().getGroupConfig().getName());
    }

    @Test
    public void groupNameIsSetToSpecifiedGroupNameWhenGroupingIsNotDisabled() {
        final String GROUP_NAME = "my-osgi-group";

        HazelcastInternalOSGiService service = getService();

        Config config = new Config();
        config.getGroupConfig().setName(GROUP_NAME);

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertNotNull(osgiInstance);
        assertEquals(GROUP_NAME,
                     osgiInstance.getConfig().getGroupConfig().getName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(GROUP_NAME,
                     instance.getConfig().getGroupConfig().getName());
    }

    @Test
    public void groupNameIsSetToDefaultGroupNameWhenGroupingIsDisabled() throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);

            HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance();
            assertNotNull(osgiInstance);
            assertEquals(GroupConfig.DEFAULT_GROUP_NAME, osgiInstance.getConfig().getGroupConfig().getName());

            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertNotNull(instance);
            assertEquals(GroupConfig.DEFAULT_GROUP_NAME, instance.getConfig().getGroupConfig().getName());
        } finally {
            if (propValue != null) {
                System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void groupNameIsSetToSpecifiedGroupNameWhenGroupingIsDisabled() throws BundleException {
        final String GROUP_NAME = "my-osgi-group";

        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);

            Config config = new Config();
            config.getGroupConfig().setName(GROUP_NAME);

            HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
            assertNotNull(osgiInstance);
            assertEquals(GROUP_NAME, osgiInstance.getConfig().getGroupConfig().getName());

            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertNotNull(instance);
            assertEquals(GROUP_NAME, instance.getConfig().getGroupConfig().getName());
        } finally {
            if (propValue != null) {
                System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED, propValue);
            }
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }

    @Test
    public void instanceRetrievedSuccessfullyWithItsName() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HazelcastInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        service.newHazelcastInstance(config);

        HazelcastOSGiInstance osgiInstance = service.getHazelcastInstanceByName(INSTANCE_NAME);
        assertNotNull(osgiInstance);
        assertEquals(config, osgiInstance.getConfig());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(config, instance.getConfig());
    }

    @Test
    public void allInstancesRetrievedSuccessfully() {
        Set<HazelcastOSGiInstance> osgiInstances = new HashSet<HazelcastOSGiInstance>();

        HazelcastInternalOSGiService service = getService();

        osgiInstances.add(service.newHazelcastInstance());
        osgiInstances.add(service.newHazelcastInstance(new Config("test-osgi-instance")));

        Set<HazelcastOSGiInstance> allOSGiInstances = service.getAllHazelcastInstances();
        assertEquals(osgiInstances.size(), allOSGiInstances.size());
        assertTrue(allOSGiInstances.containsAll(osgiInstances));
    }

    @Test
    public void instanceShutdownSuccessfully() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HazelcastInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertNotNull(osgiInstance);
        assertTrue(osgiInstance.getLifecycleService().isRunning());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertTrue(instance.getLifecycleService().isRunning());

        service.shutdownHazelcastInstance(osgiInstance);

        assertFalse(osgiInstance.getLifecycleService().isRunning());
        assertFalse(instance.getLifecycleService().isRunning());
        assertNull(service.getHazelcastInstanceByName(INSTANCE_NAME));
        assertFalse(service.getAllHazelcastInstances().contains(osgiInstance));
    }

    @Test
    public void allInstancesShutdownSuccessfully() {
        Set<HazelcastOSGiInstance> osgiInstances = new HashSet<HazelcastOSGiInstance>();

        HazelcastInternalOSGiService service = getService();

        osgiInstances.add(service.newHazelcastInstance());
        osgiInstances.add(service.newHazelcastInstance(new Config("test-osgi-instance")));

        Set<HazelcastOSGiInstance> allOSGiInstances = service.getAllHazelcastInstances();
        for (HazelcastOSGiInstance osgiInstance : allOSGiInstances) {
            assertTrue(osgiInstance.getLifecycleService().isRunning());
            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertNotNull(instance);
            assertTrue(instance.getLifecycleService().isRunning());
        }

        service.shutdownAll();

        for (HazelcastOSGiInstance osgiInstance : osgiInstances) {
            assertFalse(osgiInstance.getLifecycleService().isRunning());
            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertNotNull(instance);
            assertFalse(instance.getLifecycleService().isRunning());
        }

        allOSGiInstances = service.getAllHazelcastInstances();
        assertEquals(0, allOSGiInstances.size());
    }

}
