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

package com.hazelcast.test.starter.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.HazelcastProxyFactory;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HazelcastProxyFactoryTest {

    @Test
    public void testReturnedProxyImplements_sameInterfaceByNameOnTargetClassLoader() throws Exception {
        ProxiedInterface delegate = new ProxiedInterface() {
            @Override
            public void get() {
            }
        };

        // HazelcastAPIDelegatingClassloader will reload the bytes of ProxiedInterface as a new class
        // as happens with every com.hazelcast class that contains "test"
        HazelcastAPIDelegatingClassloader targetClassLoader
                = new HazelcastAPIDelegatingClassloader(new URL[]{}, HazelcastProxyFactoryTest.class.getClassLoader());

        Object proxy = HazelcastProxyFactory.proxyObjectForStarter(targetClassLoader, delegate);

        assertNotNull(proxy);
        Class<?>[] ifaces = proxy.getClass().getInterfaces();
        assertEquals(1, ifaces.length);

        Class<?> proxyInterface = ifaces[0];
        // it is not the same class but has the same name on a different classloader
        assertNotEquals(ProxiedInterface.class, proxyInterface);
        assertEquals(ProxiedInterface.class.getName(), proxyInterface.getName());
        assertEquals(targetClassLoader, proxyInterface.getClassLoader());
    }

    @Test
    public void testProxyHazelcastInstanceClasses_ofSameVersion_areSame() {
        HazelcastInstance hz1 = HazelcastStarter.newHazelcastInstance("4.0");
        HazelcastInstance hz2 = HazelcastStarter.newHazelcastInstance("4.0");
        try {
            assertEquals(hz1.getClass(), hz2.getClass());
        } finally {
            hz1.shutdown();
            hz2.shutdown();
        }
    }

    public interface ProxiedInterface {
        void get();
    }
}
