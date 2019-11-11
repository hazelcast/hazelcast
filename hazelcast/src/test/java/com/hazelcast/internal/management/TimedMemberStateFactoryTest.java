package com.hazelcast.internal.management;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.LocalCacheStats;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE, "");
    }

    @Test
    public void optaneMemorySupport_explicitlyEnabled() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-enabled.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertTrue(actual.isNativeMemoryEnabled());
    }

    @Test
    public void optaneMemorySupport_implicitlyDisabled() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("empty-config.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertFalse(actual.isNativeMemoryEnabled());
    }

    @Test
    public void nativeMemoryEnabledForMap() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-for-datastructures.xml");
        instance.getMap("myMap");

        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertTrue(actual.getMemberState().getLocalMapStats("myMap").isNativeMemoryUsed());
    }

    @Test
    public void nativeMemoryEnabledForCache() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-for-datastructures.xml");
        instance.getCacheManager().getCache("myCache");

        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        LocalCacheStats myCacheStats = actual.getMemberState().getLocalCacheStats("/hz/myCache");
        assertNotNull(myCacheStats);
        assertTrue(myCacheStats.isNativeMemoryUsed());
    }

}
