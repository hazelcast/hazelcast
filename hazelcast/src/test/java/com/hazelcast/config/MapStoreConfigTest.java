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

package com.hazelcast.config;

import com.hazelcast.internal.config.MapStoreConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.config.MapStoreConfig.DEFAULT_WRITE_BATCH_SIZE;
import static com.hazelcast.config.MapStoreConfig.DEFAULT_WRITE_DELAY_SECONDS;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreConfigTest extends HazelcastTestSupport {

    MapStoreConfig defaultCfg = new MapStoreConfig();
    MapStoreConfig cfgNotEnabled = new MapStoreConfig().setEnabled(false);
    MapStoreConfig cfgNotWriteCoalescing = new MapStoreConfig().setWriteCoalescing(false);
    MapStoreConfig cfgNonDefaultWriteDelaySeconds = new MapStoreConfig()
            .setWriteDelaySeconds(DEFAULT_WRITE_DELAY_SECONDS + 1);
    MapStoreConfig cfgNonDefaultWriteBatchSize = new MapStoreConfig()
            .setWriteBatchSize(DEFAULT_WRITE_BATCH_SIZE + 1);
    MapStoreConfig cfgNonNullClassName = new MapStoreConfig().setClassName("some.class");
    MapStoreConfig cfgNonNullOtherClassName = new MapStoreConfig().setClassName("some.class.other");
    MapStoreConfig cfgNonNullFactoryClassName = new MapStoreConfig().setFactoryClassName("factoryClassName");
    MapStoreConfig cfgNonNullOtherFactoryClassName = new MapStoreConfig().setFactoryClassName("some.class.other");
    MapStoreConfig cfgNonNullImplementation = new MapStoreConfig().setImplementation(new Object());
    MapStoreConfig cfgNonNullOtherImplementation = new MapStoreConfig().setImplementation(new Object());
    MapStoreConfig cfgNonNullFactoryImplementation = new MapStoreConfig().setFactoryImplementation(new Object());
    MapStoreConfig cfgNonNullOtherFactoryImplementation = new MapStoreConfig().setFactoryImplementation(new Object());
    MapStoreConfig cfgWithProperties = new MapStoreConfig().setProperty("a", "b");
    MapStoreConfig cfgEagerMode = new MapStoreConfig().setInitialLoadMode(EAGER);
    MapStoreConfig cfgNullMode = new MapStoreConfig().setInitialLoadMode(null);

    @Test
    public void getAsReadOnly() {
        MapStoreConfigReadOnly readOnlyCfg = new MapStoreConfigReadOnly(cfgNonNullClassName);
        assertEquals("some.class", readOnlyCfg.getClassName());
        assertEquals(cfgNonNullClassName, readOnlyCfg);
        // also test returning cached read only instance
        assertEquals(readOnlyCfg, new MapStoreConfigReadOnly(cfgNonNullClassName));
    }

    @Test
    public void getClassName() {
        assertNull(new MapStoreConfig().getClassName());
    }

    @Test
    public void setClassName() {
        assertEquals("some.class", cfgNonNullClassName.getClassName());
        assertEquals(new MapStoreConfig().setClassName("some.class"), cfgNonNullClassName);
    }

    @Test
    public void getFactoryClassName() {
        assertNull(new MapStoreConfig().getFactoryClassName());
    }

    @Test
    public void setFactoryClassName() {
        assertEquals("factoryClassName", cfgNonNullFactoryClassName.getFactoryClassName());
        assertEquals(new MapStoreConfig().setFactoryClassName("factoryClassName"), cfgNonNullFactoryClassName);
    }

    @Test
    public void getWriteDelaySeconds() {
        assertEquals(DEFAULT_WRITE_DELAY_SECONDS, new MapStoreConfig().getWriteDelaySeconds());
    }

    @Test
    public void setWriteDelaySeconds() {
        assertEquals(DEFAULT_WRITE_DELAY_SECONDS + 1, cfgNonDefaultWriteDelaySeconds.getWriteDelaySeconds());
        assertEquals(new MapStoreConfig().setWriteDelaySeconds(DEFAULT_WRITE_DELAY_SECONDS + 1), cfgNonDefaultWriteDelaySeconds);
    }

    @Test
    public void getWriteBatchSize() {
        assertEquals(DEFAULT_WRITE_BATCH_SIZE, new MapStoreConfig().getWriteBatchSize());
    }

    @Test
    public void setWriteBatchSize() {
        assertEquals(DEFAULT_WRITE_BATCH_SIZE + 1, cfgNonDefaultWriteBatchSize.getWriteBatchSize());
        assertEquals(new MapStoreConfig().setWriteBatchSize(DEFAULT_WRITE_BATCH_SIZE + 1), cfgNonDefaultWriteBatchSize);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setWriteBatchSize_whenLessThanOne() {
        MapStoreConfig cfg = new MapStoreConfig().setWriteBatchSize(-15);
    }

    @Test
    public void isEnabled() {
        assertTrue(new MapStoreConfig().isEnabled());
    }

    @Test
    public void setEnabled() {
        assertFalse(cfgNotEnabled.isEnabled());
        assertEquals(new MapStoreConfig().setEnabled(false), cfgNotEnabled);
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

    @Test
    public void equals_whenNull() {
        MapStoreConfig cfg = new MapStoreConfig();
        assertFalse(cfg.equals(null));
    }

    @Test
    public void equals_whenSame() {
        MapStoreConfig cfg = new MapStoreConfig();
        assertTrue(cfg.equals(cfg));
    }

    @Test
    public void equals_whenOtherClass() {
        MapStoreConfig cfg = new MapStoreConfig();
        assertFalse(cfg.equals(new Object()));
    }

    @Test
    public void testEquals() {
        assertFalse(defaultCfg.equals(cfgNotEnabled));
        assertFalse(defaultCfg.equals(cfgNotWriteCoalescing));
        assertFalse(defaultCfg.equals(cfgNonDefaultWriteDelaySeconds));
        assertFalse(defaultCfg.equals(cfgNonDefaultWriteBatchSize));

        // class name branches
        assertFalse(defaultCfg.equals(cfgNonNullClassName));
        assertFalse(cfgNonNullClassName.equals(cfgNonNullOtherClassName));
        assertFalse(cfgNonNullClassName.equals(defaultCfg));

        // factory class name branches
        assertFalse(defaultCfg.equals(cfgNonNullFactoryClassName));
        assertFalse(cfgNonNullFactoryClassName.equals(cfgNonNullOtherFactoryClassName));
        assertFalse(cfgNonNullFactoryClassName.equals(defaultCfg));

        // implementation
        assertFalse(defaultCfg.equals(cfgNonNullImplementation));
        assertFalse(cfgNonNullImplementation.equals(cfgNonNullOtherImplementation));
        assertFalse(cfgNonNullImplementation.equals(defaultCfg));

        // factory implementation
        assertFalse(defaultCfg.equals(cfgNonNullFactoryImplementation));
        assertFalse(cfgNonNullFactoryImplementation.equals(cfgNonNullOtherFactoryImplementation));
        assertFalse(cfgNonNullFactoryImplementation.equals(defaultCfg));

        assertFalse(defaultCfg.equals(cfgWithProperties));

        assertFalse(defaultCfg.equals(cfgEagerMode));
    }

    @Test
    public void testHashCode() {
        assumeDifferentHashCodes();
        assertNotEquals(defaultCfg.hashCode(), cfgNotEnabled.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgNotWriteCoalescing.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgNonNullClassName.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgNonNullFactoryClassName.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgNonNullImplementation.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgNonNullFactoryImplementation.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgEagerMode.hashCode());
        assertNotEquals(defaultCfg.hashCode(), cfgNullMode.hashCode());
    }

    @Test
    public void testToString() {
        assertContains(defaultCfg.toString(), "MapStoreConfig");
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(MapStoreConfig.class)
                .suppress(Warning.NONFINAL_FIELDS, Warning.NULL_FIELDS)
                .withPrefabValues(MapStoreConfigReadOnly.class,
                        new MapStoreConfigReadOnly(cfgNotEnabled),
                        new MapStoreConfigReadOnly(cfgNonNullClassName))
                .verify();
    }
}
