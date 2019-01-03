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

package com.hazelcast.config;

import com.hazelcast.config.CountDownLatchConfig.CountDownLatchConfigReadOnly;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CountDownLatchConfigTest extends HazelcastTestSupport {

    protected CountDownLatchConfig config = new CountDownLatchConfig();

    @Test
    public void testConstructor_withName() {
        config = new CountDownLatchConfig("myCountDownLatch");

        assertEquals("myCountDownLatch", config.getName());
    }

    @Test
    public void setName() {
        config.setName("myCountDownLatch");

        assertEquals("myCountDownLatch", config.getName());
    }

    @Test(expected = IllegalArgumentException.class)
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

        Data data = serializationService.toData(config);
        CountDownLatchConfig clone = serializationService.toObject(data);

        assertEquals(clone, config);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(CountDownLatchConfig.class)
                .allFieldsShouldBeUsedExcept("readOnly")
                .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
                .withPrefabValues(CountDownLatchConfigReadOnly.class,
                        new CountDownLatchConfigReadOnly(new CountDownLatchConfig("red")),
                        new CountDownLatchConfigReadOnly(new CountDownLatchConfig("black")))
                .verify();
    }

}
