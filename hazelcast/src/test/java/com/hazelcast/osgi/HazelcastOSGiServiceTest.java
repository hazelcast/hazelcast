/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.osgi;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.osgi.impl.HazelcastInternalOSGiService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HazelcastOSGiServiceTest extends HazelcastTestSupport {

    private TestBundle bundle;
    private TestBundleContext bundleContext;
    private TestBundleRegisterDeregisterListener registerDeregisterListener;

    private class TestBundleRegisterDeregisterListener implements TestBundle.RegisterDeregisterListener {

        final String className;

        boolean throwExceptionOnRegister;
        boolean throwExceptionOnDeregister;

        private TestBundleRegisterDeregisterListener() {
            this.className = null;
        }

        private TestBundleRegisterDeregisterListener(String className) {
            this.className = className;
        }

        @Override
        public void onRegister(String clazz, TestServiceReference serviceReference) {
            if (throwExceptionOnRegister) {
                if (className != null) {
                    if (clazz.equals(className)) {
                        throw new IllegalStateException("You cannot register!");
                    }
                } else {
                    throw new IllegalStateException("You cannot register!");
                }
            }
        }

        @Override
        public void onDeregister(String clazz, TestServiceReference serviceReference) {
            if (throwExceptionOnDeregister) {
                if (className != null) {
                    if (clazz.equals(className)) {
                        throw new IllegalStateException("You cannot deregister!");
                    }
                } else {
                    throw new IllegalStateException("You cannot deregister!");
                }
            }
        }
    }

    @Before
    public void setup() throws BundleException {
        registerDeregisterListener = new TestBundleRegisterDeregisterListener();
        bundle = new TestBundle(registerDeregisterListener);
        bundleContext = bundle.getBundleContext();
        bundle.start();
    }

    @After
    public void tearDown() throws BundleException {
        try {
            bundle.stop();
            bundle = null;
            registerDeregisterListener = null;
            bundleContext = null;
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private HazelcastInternalOSGiService getService(TestBundleContext bundleContext) {
        ServiceReference serviceRef = bundleContext.getServiceReference(HazelcastOSGiService.class.getName());
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
            assertNotNull(service);

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
    public void serviceCouldNotBeActivatedWhenThereIsExceptionWhileRegisteringDefaultInstance() throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_START);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_START, "true");

            TestBundleRegisterDeregisterListener registerDeregisterListener =
                    new TestBundleRegisterDeregisterListener(HazelcastInstance.class.getName());
            registerDeregisterListener.throwExceptionOnRegister = true;

            testBundle = new TestBundle(registerDeregisterListener);
            try {
                testBundle.start();
                fail("OSGI service could not be activated because of exception while registering default instance."
                        + " It is expected to get `IllegalStateException` here!");
            } catch (IllegalStateException e) {
                // Since bundle is not active, it is expected to get `IllegalStateException`
            }
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
    public void serviceCouldNotBeActivatedWhenThereIsExceptionWhileRegisteringService() throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_START);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_START, "true");

            TestBundleRegisterDeregisterListener registerDeregisterListener =
                    new TestBundleRegisterDeregisterListener(HazelcastOSGiService.class.getName());
            registerDeregisterListener.throwExceptionOnRegister = true;

            testBundle = new TestBundle(registerDeregisterListener);
            try {
                testBundle.start();
                fail("OSGI service could not be activated because of exception while registering default instance."
                        + "It is expected to get `IllegalStateException` here!");
            } catch (IllegalStateException e) {
                // Since bundle is not active, it is expected to get `IllegalStateException`
            }
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
        assertEquals(config.getInstanceName(), osgiInstance.getConfig().getInstanceName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(config.getInstanceName(), instance.getConfig().getInstanceName());
    }

    @Test
    public void newInstanceRegisteredAsServiceWhenRegistrationIsNotDisabled() throws BundleException {
        HazelcastInternalOSGiService service = getService();

        service.newHazelcastInstance();

        assertNull(bundleContext.getServiceReference(HazelcastOSGiInstance.class.getName()));
    }

    @Test
    public void newInstanceNotRegisteredAsServiceWhenRegistrationIsDisabled() throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_REGISTER_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_REGISTER_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

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
    public void clusterNameIsSetToDefaultClusterNameOfBundleWhenGroupingIsNotDisabled() {
        HazelcastInternalOSGiService service = getService();

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance();
        assertEquals(HazelcastInternalOSGiService.DEFAULT_CLUSTER_NAME,
                osgiInstance.getConfig().getClusterName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(HazelcastInternalOSGiService.DEFAULT_CLUSTER_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameOfBundleWhenConfigIsGivenWithoutSpecifiedClusterConfigAndGroupingIsNotDisabled() {
        Config config = new Config();

        HazelcastInternalOSGiService service = getService();

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertEquals(HazelcastInternalOSGiService.DEFAULT_CLUSTER_NAME,
                osgiInstance.getConfig().getClusterName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(HazelcastInternalOSGiService.DEFAULT_CLUSTER_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameOfBundleWhenConfigIsGivenWithNullGroupConfigAndGroupingIsNotDisabled() {
        Config config = new Config();

        HazelcastInternalOSGiService service = getService();

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertEquals(HazelcastInternalOSGiService.DEFAULT_CLUSTER_NAME,
                osgiInstance.getConfig().getClusterName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(HazelcastInternalOSGiService.DEFAULT_CLUSTER_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToSpecifiedClusterNameWhenGroupingIsNotDisabled() {
        final String GROUP_NAME = "my-osgi-group";

        HazelcastInternalOSGiService service = getService();

        Config config = new Config();
        config.setClusterName(GROUP_NAME);

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertEquals(GROUP_NAME,
                osgiInstance.getConfig().getClusterName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertEquals(GROUP_NAME,
                instance.getConfig().getClusterName());
    }

    @Test
    public void clusterNameIsSetToDefaultClusterNameWhenGroupingIsDisabled() throws BundleException {
        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance();
            assertEquals(Config.DEFAULT_CLUSTER_NAME, osgiInstance.getConfig().getClusterName());

            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertEquals(Config.DEFAULT_CLUSTER_NAME, instance.getConfig().getClusterName());
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
    public void clusterNameIsSetToSpecifiedClusterNameWhenGroupingIsDisabled() throws BundleException {
        final String CLUSTER_NAME = "my-osgi-cluster";

        String propValue = System.getProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED);
        TestBundle testBundle = null;
        try {
            System.setProperty(HazelcastOSGiService.HAZELCAST_OSGI_GROUPING_DISABLED, "true");

            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            Config config = new Config();
            config.setClusterName(CLUSTER_NAME);

            HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
            assertEquals(CLUSTER_NAME, osgiInstance.getConfig().getClusterName());

            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertEquals(CLUSTER_NAME, instance.getConfig().getClusterName());
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
        assertEquals(config.getInstanceName(), osgiInstance.getConfig().getInstanceName());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertNotNull(instance);
        assertEquals(config.getInstanceName(), instance.getConfig().getInstanceName());
    }

    @Test
    public void allInstancesRetrievedSuccessfully() {
        Set<HazelcastOSGiInstance> osgiInstances = new HashSet<HazelcastOSGiInstance>();

        HazelcastInternalOSGiService service = getService();

        osgiInstances.add(service.newHazelcastInstance());
        osgiInstances.add(service.newHazelcastInstance(new Config("test-osgi-instance")));

        Set<HazelcastOSGiInstance> allOSGiInstances = service.getAllHazelcastInstances();
        assertEquals(osgiInstances.size(), allOSGiInstances.size());
        assertContainsAll(allOSGiInstances, osgiInstances);
    }

    @Test
    public void instanceShutdownSuccessfully() {
        final String INSTANCE_NAME = "test-osgi-instance";

        HazelcastInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertTrue(osgiInstance.getLifecycleService().isRunning());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
        assertTrue(instance.getLifecycleService().isRunning());

        service.shutdownHazelcastInstance(osgiInstance);

        assertFalse(osgiInstance.getLifecycleService().isRunning());
        assertFalse(instance.getLifecycleService().isRunning());
        assertNull(service.getHazelcastInstanceByName(INSTANCE_NAME));
        assertFalse(service.getAllHazelcastInstances().contains(osgiInstance));
    }

    @Test
    public void instanceShutdownSuccessfullyAlthoughThereIsExceptionWhileDeregister() {
        final String INSTANCE_NAME = "test-osgi-instance";

        registerDeregisterListener.throwExceptionOnDeregister = true;

        HazelcastInternalOSGiService service = getService();

        Config config = new Config(INSTANCE_NAME);

        HazelcastOSGiInstance osgiInstance = service.newHazelcastInstance(config);
        assertTrue(osgiInstance.getLifecycleService().isRunning());

        HazelcastInstance instance = osgiInstance.getDelegatedInstance();
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
            assertTrue(instance.getLifecycleService().isRunning());
        }

        service.shutdownAll();

        for (HazelcastOSGiInstance osgiInstance : osgiInstances) {
            assertFalse(osgiInstance.getLifecycleService().isRunning());
            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertFalse(instance.getLifecycleService().isRunning());
        }

        allOSGiInstances = service.getAllHazelcastInstances();
        assertEquals(0, allOSGiInstances.size());
    }

    @Test
    public void allInstancesShutdownSuccessfullyAlthoughThereIsExceptionWhileDeregister() {
        Set<HazelcastOSGiInstance> osgiInstances = new HashSet<HazelcastOSGiInstance>();

        registerDeregisterListener.throwExceptionOnDeregister = true;

        HazelcastInternalOSGiService service = getService();

        osgiInstances.add(service.newHazelcastInstance());
        osgiInstances.add(service.newHazelcastInstance(new Config("test-osgi-instance")));

        Set<HazelcastOSGiInstance> allOSGiInstances = service.getAllHazelcastInstances();
        for (HazelcastOSGiInstance osgiInstance : allOSGiInstances) {
            assertTrue(osgiInstance.getLifecycleService().isRunning());
            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertTrue(instance.getLifecycleService().isRunning());
        }

        service.shutdownAll();

        for (HazelcastOSGiInstance osgiInstance : osgiInstances) {
            assertFalse(osgiInstance.getLifecycleService().isRunning());
            HazelcastInstance instance = osgiInstance.getDelegatedInstance();
            assertFalse(instance.getLifecycleService().isRunning());
        }

        allOSGiInstances = service.getAllHazelcastInstances();
        assertEquals(0, allOSGiInstances.size());
    }

    @Test
    public void serviceIsNotOperationalWhenItIsNotActive() throws BundleException {
        TestBundle testBundle = null;
        try {
            testBundle = new TestBundle();
            TestBundleContext testBundleContext = testBundle.getBundleContext();

            testBundle.start();

            HazelcastInternalOSGiService service = getService(testBundleContext);
            assertNotNull(service);

            testBundle.stop();
            testBundle = null;

            try {
                service.newHazelcastInstance();
                fail("OSGI service is not active so it is not in operation mode."
                        + " It is expected to get `IllegalStateException` here!");
            } catch (IllegalStateException e) {
                // since the bundle is not active, it is expected to get `IllegalStateException`
            }
        } finally {
            if (testBundle != null) {
                testBundle.stop();
            }
        }
    }
}
