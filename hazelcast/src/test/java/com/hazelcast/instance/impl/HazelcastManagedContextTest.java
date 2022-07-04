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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.internal.services.NodeAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastManagedContextTest extends HazelcastTestSupport {

    private DependencyInjectionUserClass userClass;
    private DependencyInjectionUserManagedContext userContext;
    private HazelcastInstance hazelcastInstance;
    private Node node;
    private SerializationService serializationService;

    @Before
    public void setUp() {
        userClass = new DependencyInjectionUserClass();
        userContext = new DependencyInjectionUserManagedContext();

        Config config = getConfig()
                .setManagedContext(userContext);

        hazelcastInstance = createHazelcastInstance(config);
        node = getNode(hazelcastInstance);
        serializationService = getSerializationService(hazelcastInstance);
    }

    @Test
    public void testInitialize() {
        DependencyInjectionUserClass initializedUserClass = (DependencyInjectionUserClass) serializationService.getManagedContext().initialize(userClass);

        assertEquals(hazelcastInstance, initializedUserClass.hazelcastInstance);
        assertEquals(node, initializedUserClass.node);
        assertEquals(serializationService, initializedUserClass.serializationService);
        assertTrue(userContext.wasCalled);
    }

    private static class DependencyInjectionUserClass implements HazelcastInstanceAware, NodeAware, SerializationServiceAware {

        private HazelcastInstance hazelcastInstance;
        private Node node;
        private SerializationService serializationService;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void setNode(Node node) {
            this.node = node;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.serializationService = serializationService;
        }
    }

    private static class DependencyInjectionUserManagedContext implements ManagedContext {

        private boolean wasCalled;

        @Override
        public Object initialize(Object obj) {
            wasCalled = true;
            return obj;
        }
    }
}
