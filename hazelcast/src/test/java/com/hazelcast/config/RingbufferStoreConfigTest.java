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

import com.hazelcast.internal.config.RingbufferStoreConfigReadOnly;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.ringbuffer.RingbufferStoreFactory;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.ringbuffer.impl.RingbufferStoreWrapper;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferStoreConfigTest {

    private RingbufferStoreConfig config = new RingbufferStoreConfig();

    @Test
    public void testDefaultSetting() {
        assertTrue(config.isEnabled());
        assertNull(config.getClassName());
        assertNull(config.getFactoryClassName());
        assertNull(config.getFactoryImplementation());
        assertNull(config.getStoreImplementation());
        assertNotNull(config.getProperties());
        assertTrue(config.getProperties().isEmpty());
    }

    @Test
    public void setStoreImplementation() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        RingbufferStore<Data> store = RingbufferStoreWrapper.create(
                RingbufferService.getRingbufferNamespace("name"),
                config, OBJECT, serializationService, null);

        config.setStoreImplementation(store);

        assertEquals(store, config.getStoreImplementation());
    }

    @Test
    public void setProperties() {
        Properties properties = new Properties();
        properties.put("key", "value");

        config.setProperties(properties);

        assertEquals(properties, config.getProperties());
    }

    @Test
    public void setProperty() {
        config.setProperty("key", "value");

        assertEquals("value", config.getProperty("key"));
    }

    @Test
    public void setFactoryClassName() {
        config.setFactoryClassName("myFactoryClassName");

        assertEquals("myFactoryClassName", config.getFactoryClassName());
    }

    @Test
    public void setFactoryImplementation() {
        RingbufferStoreFactory factory = new RingbufferStoreFactory() {
            @Override
            public RingbufferStore newRingbufferStore(String name, Properties properties) {
                return null;
            }
        };

        config.setFactoryImplementation(factory);

        assertEquals(factory, config.getFactoryImplementation());
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(RingbufferStoreConfig.class)
                      .suppress(Warning.NONFINAL_FIELDS)
                      .withPrefabValues(RingbufferStoreConfigReadOnly.class,
                              new RingbufferStoreConfigReadOnly(new RingbufferStoreConfig().setClassName("red")),
                              new RingbufferStoreConfigReadOnly(new RingbufferStoreConfig().setClassName("black")))
                      .verify();
    }
}
