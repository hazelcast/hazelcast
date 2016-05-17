package com.hazelcast.config;

import com.hazelcast.core.MapStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.junit.Assert.*;

/**
 * Test MapStoreConfig
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapStoreConfigTest {


    @Test
    public void getAsReadOnly() {
        MapStoreConfig cfg = new MapStoreConfig().setClassName("mapStoreClassName");
        MapStoreConfigReadOnly readOnlyCfg = cfg.getAsReadOnly();
        assertEquals("mapStoreClassName", readOnlyCfg.getClassName());
        assertEquals(cfg, readOnlyCfg);
    }

    @Test
    public void getClassName() {
        assertNull(new MapStoreConfig().getClassName());
    }

    @Test
    public void setClassName() {
        MapStoreConfig cfg = new MapStoreConfig().setClassName("mapStoreClassName");
        assertEquals("mapStoreClassName", cfg.getClassName());
        assertEquals(new MapStoreConfig().setClassName("mapStoreClassName"), cfg);
    }

    @Test
    public void getFactoryClassName() {
        assertNull(new MapStoreConfig().getFactoryClassName());
    }

    @Test
    public void setFactoryClassName() {
        MapStoreConfig cfg = new MapStoreConfig().setFactoryClassName("factoryClassName");
        assertEquals("factoryClassName", cfg.getFactoryClassName());
        assertEquals(new MapStoreConfig().setFactoryClassName("factoryClassName"), cfg);
    }

    @Test
    public void getWriteDelaySeconds() {
        assertEquals(MapStoreConfig.DEFAULT_WRITE_DELAY_SECONDS, new MapStoreConfig().getWriteDelaySeconds());
    }

    @Test
    public void setWriteDelaySeconds()
            throws Exception {
        MapStoreConfig cfg = new MapStoreConfig().setWriteDelaySeconds(199);
        assertEquals(199, cfg.getWriteDelaySeconds());
        assertEquals(new MapStoreConfig().setWriteDelaySeconds(199), cfg);
    }

    @Test
    public void getWriteBatchSize() {
        assertEquals(MapStoreConfig.DEFAULT_WRITE_BATCH_SIZE, new MapStoreConfig().getWriteBatchSize());
    }

    @Test
    public void setWriteBatchSize() {
        MapStoreConfig cfg = new MapStoreConfig().setWriteBatchSize(399);
        assertEquals(399, cfg.getWriteBatchSize());
        assertEquals(new MapStoreConfig().setWriteBatchSize(399), cfg);
    }

    @Test
    public void isEnabled() {
        assertTrue(new MapStoreConfig().isEnabled());
    }

    @Test
    public void setEnabled() {
        MapStoreConfig cfg = new MapStoreConfig().setEnabled(false);
        assertFalse(cfg.isEnabled());
        assertEquals(new MapStoreConfig().setEnabled(false), cfg);
    }

    @Test
    public void setImplementation() {
        Object mapStoreImpl = new Object();
        MapStoreConfig cfg = new MapStoreConfig().setImplementation(mapStoreImpl);
        assertEquals(mapStoreImpl, cfg.getImplementation());
        assertEquals(new MapStoreConfig().setImplementation(mapStoreImpl), cfg);
    }

    @Test
    public void getImplementation() {
        assertNull(new MapStoreConfig().getImplementation());
    }

    @Test
    public void setFactoryImplementation() {
        Object mapStoreFactoryImpl = new Object();
        MapStoreConfig cfg = new MapStoreConfig().setFactoryImplementation(mapStoreFactoryImpl);
        assertEquals(mapStoreFactoryImpl, cfg.getFactoryImplementation());
        assertEquals(new MapStoreConfig().setFactoryImplementation(mapStoreFactoryImpl), cfg);
    }

    @Test
    public void getFactoryImplementation() {
        assertNull(new MapStoreConfig().getFactoryImplementation());
    }

    @Test
    public void setProperty() {
        MapStoreConfig cfg = new MapStoreConfig().setProperty("a", "b");
        assertEquals("b", cfg.getProperty("a"));
        assertEquals(new MapStoreConfig().setProperty("a", "b"), cfg);
    }

    @Test
    public void getProperty() {
        assertNull(new MapStoreConfig().getProperty("a"));
    }

    @Test
    public void getProperties() {
        assertEquals(new Properties(), new MapStoreConfig().getProperties());
    }

    @Test
    public void setProperties() {
        Properties properties = new Properties();
        properties.put("a", "b");
        MapStoreConfig cfg = new MapStoreConfig().setProperties(properties);
        assertEquals(properties, cfg.getProperties());
        assertEquals("b", cfg.getProperty("a"));
        Properties otherProperties = new Properties();
        otherProperties.put("a", "b");
        assertEquals(new MapStoreConfig().setProperties(otherProperties), cfg);
    }

    @Test
    public void getInitialLoadMode() {
        assertEquals(LAZY, new MapStoreConfig().getInitialLoadMode());
    }

    @Test
    public void setInitialLoadMode() {
        MapStoreConfig cfg = new MapStoreConfig().setInitialLoadMode(EAGER);
        assertEquals(EAGER, cfg.getInitialLoadMode());
        assertEquals(new MapStoreConfig().setInitialLoadMode(EAGER), cfg);
    }

    @Test
    public void isWriteCoalescing() {
        assertEquals(MapStoreConfig.DEFAULT_WRITE_COALESCING, new MapStoreConfig().isWriteCoalescing());
    }

    @Test
    public void setWriteCoalescing() {
        MapStoreConfig cfg = new MapStoreConfig();
        cfg.setWriteCoalescing(false);
        assertFalse(cfg.isWriteCoalescing());
        MapStoreConfig otherCfg = new MapStoreConfig();
        otherCfg.setWriteCoalescing(false);
        assertEquals(otherCfg, cfg);
    }

}
