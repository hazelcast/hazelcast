/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Map;
import java.util.Set;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FalsePredicateTest extends HazelcastTestSupport {

    private FalsePredicate falsePredicate;
    private SerializationService serializationService;

    @Before
    public void setup() {
        falsePredicate = new FalsePredicate();
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void apply() {
        Map.Entry entry = mock(Map.Entry.class);
        boolean result = falsePredicate.apply(entry);
        assertFalse(result);
    }

    @Test
    public void isIndexed() {
        QueryContext queryContext = mock(QueryContext.class);

        assertTrue(falsePredicate.isIndexed(queryContext));
    }

    @Test
    public void filter() {
        QueryContext queryContext = mock(QueryContext.class);

        Set<QueryableEntry> result = falsePredicate.filter(queryContext);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void serialize() {
        Data data = serializationService.toData(falsePredicate);
        Object result = serializationService.toObject(data);
        assertInstanceOf(FalsePredicate.class, result);
    }

    @Test
    public void testToString() {
        String result = falsePredicate.toString();
        assertEquals("FalsePredicate{}", result);
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(FalsePredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .allFieldsShouldBeUsed()
            .verify();
    }
}
