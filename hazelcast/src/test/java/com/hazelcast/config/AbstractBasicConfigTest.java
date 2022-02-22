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

import com.hazelcast.internal.config.AbstractBasicConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractBasicConfigTest<T extends AbstractBasicConfig> extends HazelcastTestSupport {

    protected T config;

    @Before
    public final void setUpConfig() {
        config = createConfig();
    }

    protected abstract T createConfig();

    @Test
    public void setName() {
        config.setName("myAtomicLong");

        assertEquals("myAtomicLong", config.getName());
    }

    @Test(expected = NullPointerException.class)
    public void setName_withNull() {
        config.setName(null);
    }

    @Test
    public void testToString() {
        String configString = config.toString();
        assertNotNull("toString() should be implemented", configString);
        assertTrue("toString() should contain name field", configString.contains("name='" + config.getName() + "'"));
    }

    @Test
    public void testSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        config.setName("myAtomicLong");

        Data data = serializationService.toData(config);
        T clone = serializationService.toObject(data);

        assertEquals(clone, config);
    }
}
