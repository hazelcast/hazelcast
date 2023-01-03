package com.hazelcast.config.alto;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.alto.AltoConfigTestUtil.getEventloopCount;
import static com.hazelcast.config.alto.AltoConfigTestUtil.isTpcEnabled;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AltoConfigTest extends HazelcastTestSupport {
    private final Config config = smallInstanceConfig();

    @Test
    public void testTPCDisabledByDefault() {
        assertFalse(isTpcEnabled(createHazelcastInstance()));
    }

    @Test
    public void testEventloopCountDefault() {
        config.getAltoConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(Runtime.getRuntime().availableProcessors(), getEventloopCount(hz));
    }

    @Test
    public void testEventloopCount() {
        config.getAltoConfig().setEnabled(true).setEventloopCount(7);
        HazelcastInstance hz = createHazelcastInstance(config);
        assertTrue(isTpcEnabled(hz));
        assertEquals(7, getEventloopCount(hz));
    }

    @Test
    public void testConfigBounds() {
        AltoConfig altoConfig = config.getAltoConfig();
        assertThrows(InvalidConfigurationException.class, () -> altoConfig.setEventloopCount(0));
        assertThrows(InvalidConfigurationException.class, () -> altoConfig.setEventloopCount(256 + 1));
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(AltoConfig.class)
                .usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
