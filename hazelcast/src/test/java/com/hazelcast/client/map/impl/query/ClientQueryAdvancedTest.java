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

package com.hazelcast.client.map.impl.query;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.query.Predicates.equal;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryAdvancedTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    /**
     * Test for issue #5807.
     */
    @Test
    public void queryIndexedComparableField_whenEqualsPredicateWithNullValueIsUsed_thenConverterUsesNullObject() {
        hazelcastFactory.newHazelcastInstance(getConfig());
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(getClientConfig());
        IMap<Integer, SampleTestObjects.Value> map = client.getMap("default");

        map.addIndex(IndexType.HASH, "type");

        SampleTestObjects.ValueType valueType = new SampleTestObjects.ValueType("name");
        SampleTestObjects.Value valueWithoutNull = new SampleTestObjects.Value("notNull", valueType, 1);
        SampleTestObjects.Value valueWithNull = new SampleTestObjects.Value("null", null, 1);
        map.put(1, valueWithoutNull);
        map.put(2, valueWithNull);

        Predicate nullPredicate = equal("type", null);
        Collection<SampleTestObjects.Value> emptyFieldValues = map.values(nullPredicate);
        assertThat(emptyFieldValues, hasSize(1));
        assertThat(emptyFieldValues, contains(valueWithNull));
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }
}
