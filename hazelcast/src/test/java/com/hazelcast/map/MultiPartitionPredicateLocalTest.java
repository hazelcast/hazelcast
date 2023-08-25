/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map;

import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiPartitionPredicateLocalTest extends MultiPartitionPredicateTestSupport {

    private HazelcastInstance instance;

    @Override
    protected IMap<String, Integer> getMap(String name) {
        return instance.getMap(name);
    }

    @Override
    protected void setupInternal() {
        instance = getFactory().getAllHazelcastInstances().stream().findFirst().get();
    }

    @Override
    protected HazelcastInstance getInstance() {
        return instance;
    }

    @Test
    public void testSerialization() {
        SerializationService serializationService = getSerializationService(getInstance());
        Data serialized = serializationService.toData(getPredicate());
        PartitionPredicate deserialized = serializationService.toObject(serialized);

        assertEquals(getPartitionKeys(), deserialized.getPartitionKeys());
        assertEquals(Predicates.alwaysTrue(), deserialized.getTarget());
    }
}
