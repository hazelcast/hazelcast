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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.util.RootCauseMatcher;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyRegistry;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributedObjectTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMap() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getMap("test");
        test(instance, object);
    }

    @Test
    public void testQueue() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getQueue("test");
        test(instance, object);
    }

    @Test
    public void testTopic() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getTopic("test");
        test(instance, object);
    }

    @Test
    public void testMultiMap() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getMultiMap("test");
        test(instance, object);
    }

    @Test
    public void testSet() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getSet("test");
        test(instance, object);
    }

    @Test
    public void testList() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getList("test");
        test(instance, object);
    }

    @Test
    public void testExecutorService() {
        HazelcastInstance instance = createHazelcastInstance();
        DistributedObject object = instance.getExecutorService("test");
        test(instance, object);
    }

    private void test(HazelcastInstance instance, DistributedObject object) {
        DistributedObject object2 = instance.getDistributedObject(object.getServiceName(), object.getName());
        assertEquals(object.getServiceName(), object2.getServiceName());
        assertEquals(object.getName(), object2.getName());
        assertEquals(object, object2);
        assertContains(instance.getDistributedObjects(), object);
    }

    @Test
    public void testCustomObject() {
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setImplementation(new TestInitializingObjectService())
                        .setEnabled(true).setName(TestInitializingObjectService.NAME)
        );
        HazelcastInstance instance = createHazelcastInstance(config);
        TestInitializingObject object = instance
                .getDistributedObject(TestInitializingObjectService.NAME, "test-object");
        test(instance, object);
    }

    @Test
    public void testInitialization() {
        int nodeCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setImplementation(new TestInitializingObjectService())
                        .setEnabled(true).setName(TestInitializingObjectService.NAME)
        );

        String serviceName = TestInitializingObjectService.NAME;
        String objectName = "test-object";

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(config);
            TestInitializingObject obj2 = instances[i].getDistributedObject(serviceName, objectName);
            assertTrue(obj2.init.get());
            assertFalse(obj2.error);
        }
    }

    @Test
    public void testInitialization_whenEachNodeExecutesPostJoinOperations() {
        int nodeCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setImplementation(new TestInitializingObjectService())
                        .setEnabled(true).setName(TestInitializingObjectService.NAME)
        );

        String serviceName = TestInitializingObjectService.NAME;
        String objectName = "test-object";

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(config);
            instances[i].getDistributedObject(serviceName, objectName);
        }

        for (int i = 0; i < nodeCount; i++) {
            NodeEngine nodeEngine = getNodeEngineImpl(instances[i]);
            OperationService operationService = nodeEngine.getOperationService();
            ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
            Operation postJoinOperation = proxyService.getPostJoinOperation();

            for (int j = 0; j < nodeCount; j++) {
                if (i == j) {
                    continue;
                }
                Node node2 = getNode(instances[j]);
                operationService.send(postJoinOperation, node2.address);
            }
        }

        for (HazelcastInstance instance : instances) {
            TestInitializingObject obj = instance.getDistributedObject(serviceName, objectName);
            assertTrue(obj.init.get());
            assertFalse(obj.error);
        }
    }

    @Test
    public void testDistributedObjectDestroyed_whenDestroyDuringInitialization()
            throws InterruptedException, ExecutionException {
        final CountDownLatch initializationStarted = new CountDownLatch(1);
        final CountDownLatch objectDestroyed = new CountDownLatch(1);
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig()
                        .setEnabled(true)
                        .setName(TestInitializingObjectService.NAME)
                        .setImplementation(new TestInitializingObjectService(() -> {
                                initializationStarted.countDown();
                                try {
                                    objectDestroyed.await();
                                } catch (InterruptedException e) {
                                    ignore(e);
                                }
                        })));

        String serviceName = TestInitializingObjectService.NAME;
        String objectName = "test-object";

        HazelcastInstance instance = createHazelcastInstance(config);
        Future f = spawn(() -> {
            // must fail with DistributedObjectDestroyedException
            instance.getDistributedObject(serviceName, objectName);
        });
        initializationStarted.await();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        UUID source = nodeEngine.getLocalMember().getUuid();
        nodeEngine.getProxyService().destroyDistributedObject(serviceName, objectName, source);
        objectDestroyed.countDown();

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(new RootCauseMatcher(DistributedObjectDestroyedException.class));
        f.get();
    }

    private static class TestInitializingObjectService implements RemoteService {

        static final String NAME = "TestInitializingObjectService";
        private final Runnable initializer;

        TestInitializingObjectService() {
            this.initializer = null;
        }

        TestInitializingObjectService(Runnable initializer) {
            this.initializer = initializer;
        }

        @Override
        public DistributedObject createDistributedObject(final String objectName, UUID source, boolean local) {
            if (initializer != null) {
                return new TestInitializingObject(objectName, initializer);
            } else {
                return new TestInitializingObject(objectName);
            }
        }

        @Override
        public void destroyDistributedObject(final String objectName, boolean local) {
        }
    }

    private static class TestInitializingObject implements DistributedObject, InitializingObject {

        private final AtomicBoolean init = new AtomicBoolean(false);
        private final String name;
        private volatile boolean error;
        private final Runnable initializer;

        TestInitializingObject(final String name, Runnable initializer) {
            this.name = name;
            this.initializer = initializer;
        }

        TestInitializingObject(final String name) {
            this.name = name;
            this.initializer = null;
        }

        @Override
        public void initialize() {
            if (!init.compareAndSet(false, true)) {
                error = true;
                throw new IllegalStateException("InitializingObject must be initialized only once!");
            }
            if (initializer != null) {
                initializer.run();
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getServiceName() {
            return TestInitializingObjectService.NAME;
        }

        @Override
        public String getPartitionKey() {
            return getName();
        }

        @Override
        public void destroy() {
        }
    }

    @Test(expected = HazelcastException.class)
    public void testFailingInitialization() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setImplementation(new FailingInitializingObjectService())
                        .setEnabled(true).setName(FailingInitializingObjectService.NAME)
        );

        String serviceName = FailingInitializingObjectService.NAME;
        String objectName = "test-object";

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hz.getDistributedObject(serviceName, objectName);
    }

    @Test
    public void testFailingInitialization_whenGetProxyCalledByMultipleThreads() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setImplementation(new FailingInitializingObjectService())
                        .setEnabled(true).setName(FailingInitializingObjectService.NAME)
        );

        final String serviceName = FailingInitializingObjectService.NAME;
        final String objectName = "test-object";
        final HazelcastInstance hz = factory.newHazelcastInstance(config);

        int threads = 3;
        final CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            new Thread() {
                public void run() {
                    for (int j = 0; j < 1000; j++) {
                        try {
                            hz.getDistributedObject(serviceName, objectName);
                            fail("Proxy creation should fail!");
                        } catch (HazelcastException expected) {
                            ignore(expected);
                        }
                        LockSupport.parkNanos(1);
                    }
                    latch.countDown();
                }
            }.start();
        }
        assertOpenEventually(latch, 30);
    }

    @Test
    public void testProxyCreation_whenLocalOnly() {
        int nodeCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(
                new ServiceConfig().setImplementation(new TestInitializingObjectService())
                                   .setEnabled(true).setName(TestInitializingObjectService.NAME)
        );

        String serviceName = TestInitializingObjectService.NAME;
        String objectName = "test-object";

        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        ProxyRegistry[] registries = new ProxyRegistry[nodeCount];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = factory.newHazelcastInstance(config);
            NodeEngine nodeEngine = getNodeEngineImpl(instances[i]);
            ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
            registries[i] = proxyService.getOrCreateRegistry(serviceName);
        }

        for (int i = 0; i < instances.length; i++) {
            NodeEngine nodeEngine = getNodeEngineImpl(instances[i]);
            UUID source = nodeEngine.getLocalMember().getUuid();
            registries[i].createProxy(objectName, source, true, true);
            for (int j = i + 1; j < instances.length; j++) {
                Collection<DistributedObject> objects = new ArrayList<>();
                registries[j].getDistributedObjects(objects);
                assertTrue(objects.isEmpty());
            }
        }
    }

    private static class FailingInitializingObjectService implements RemoteService {

        static final String NAME = "FailingInitializingObjectService";

        @Override
        public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
            throw new HazelcastException("Object creation is not allowed!");
        }

        @Override
        public void destroyDistributedObject(final String objectName, boolean local) {
        }
    }
}
