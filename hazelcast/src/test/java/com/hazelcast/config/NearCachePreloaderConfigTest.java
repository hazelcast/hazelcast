package com.hazelcast.config;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCachePreloaderConfigTest {

    private NearCachePreloaderConfig config = new NearCachePreloaderConfig();

    @Test
    public void testConstructor_withDirectory() {
        config = new NearCachePreloaderConfig("myParentDirectory");

        assertTrue(config.isEnabled());
        assertEquals("myParentDirectory", config.getDirectory());
    }

    @Test
    public void setDirectory() {
        config.setDirectory("myParentDirectory");

        assertEquals("myParentDirectory", config.getDirectory());
    }

    @Test(expected = NullPointerException.class)
    public void setDirectory_withNull() {
        config.setDirectory(null);
    }

    @Test
    public void setStoreInitialDelaySeconds() {
        config.setStoreInitialDelaySeconds(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreInitialDelaySeconds_withZero() {
        config.setStoreInitialDelaySeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreInitialDelaySeconds_withNegative() {
        config.setStoreInitialDelaySeconds(-1);
    }

    @Test
    public void setStoreIntervalSeconds() {
        config.setStoreIntervalSeconds(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreIntervalSeconds_withZero() {
        config.setStoreIntervalSeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreIntervalSeconds_withNegative() {
        config.setStoreIntervalSeconds(-1);
    }

    @Test
    public void testSerialization() {
        config.setEnabled(true);
        config.setDirectory("myParentDirectory");
        config.setStoreInitialDelaySeconds(23);
        config.setStoreIntervalSeconds(42);

        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(config);
        NearCachePreloaderConfig deserialized = serializationService.toObject(serialized);

        assertEquals(config.isEnabled(), deserialized.isEnabled());
        assertEquals(config.getDirectory(), deserialized.getDirectory());
        assertEquals(config.getStoreInitialDelaySeconds(), deserialized.getStoreInitialDelaySeconds());
        assertEquals(config.getStoreIntervalSeconds(), deserialized.getStoreIntervalSeconds());
        assertEquals(config.toString(), deserialized.toString());
    }
}
