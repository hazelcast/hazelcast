/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.LocalCacheStats;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TimedMemberStateFactoryTest
        extends HazelcastTestSupport {

    HazelcastInstance instance;

    private TimedMemberStateFactory createTimedMemberStateFactory(String xmlConfigRelativeFileName) {
        Config config = new ClasspathXmlConfig("com/hazelcast/internal/management/" + xmlConfigRelativeFileName);
        instance = createHazelcastInstance(config);
        return new TimedMemberStateFactory(getHazelcastInstanceImpl(instance));
    }

    @Before
    public void before() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE, "true");
    }

    @After
    public void after() {
        if (instance != null) {
            instance.shutdown();
        }
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE);
    }

    @Test
    public void optaneMemorySupport_explicitlyEnabled() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-enabled.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertTrue(actual.isPersistentMemoryUsed());
    }

    @Test
    public void nativeMemoryWithoutPersistentMemoryDir() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-without-persistent-memory.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertFalse(actual.isPersistentMemoryUsed());
    }

    @Test
    public void optaneMemorySupport_implicitlyDisabled() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("empty-config.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertFalse(actual.isPersistentMemoryUsed());
    }

    @Test
    public void nativeMemoryEnabledForMap() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-for-datastructures.xml");
        instance.getMap("myMap");

        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertSame(NATIVE, actual.getMemberState().getLocalMapStats("myMap").getInMemoryFormat());
    }

    @Test
    public void nativeMemoryEnabledForCache() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-for-datastructures.xml");
        instance.getCacheManager().getCache("myCache");

        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        LocalCacheStats myCacheStats = actual.getMemberState().getLocalCacheStats("/hz/myCache");
        assertNotNull(myCacheStats);
        assertSame(NATIVE, myCacheStats.getInMemoryFormat());
    }

}
