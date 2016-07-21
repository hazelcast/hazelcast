package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;
import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class PluggableMemoryInfoAccessorTest extends HazelcastTestSupport {

    private static final String HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL = "hazelcast.memory.info.accessor.impl";

    private String previousValue;

    @Before
    public void setUp() throws Exception {
        previousValue = System.getProperty(HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL);

        setProperty(HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL, ZeroMemoryInfoAccessor.class.getCanonicalName());
    }

    @After
    public void tearDown() throws Exception {
        if (previousValue != null) {
            setProperty(HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL, previousValue);
        } else {
            clearProperty(HAZELCAST_MEMORY_INFO_ACCESSOR_IMPL);
        }
    }

    /**
     * Used {@link ZeroMemoryInfoAccessor} to evict every put, map should not contain any entry after this test run.
     */
    @Test
    public void testPluggedMemoryInfoAccessorUsed() throws Exception {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(FREE_HEAP_PERCENTAGE);
        maxSizeConfig.setSize(50);

        Config config = getConfig();
        config.getMapConfig("test").setEvictionPolicy(LFU).setMaxSizeConfig(maxSizeConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("test");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        assertEquals(0, map.size());
    }


}
