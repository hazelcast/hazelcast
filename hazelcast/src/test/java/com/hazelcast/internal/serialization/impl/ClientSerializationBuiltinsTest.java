/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.impl.BinaryInterface;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for classes which are used as Data in the client protocol. Changing these tests may break client compatibility.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientSerializationBuiltinsTest {

    //This list should reflect the builtin classes that client depend on.
    // New builtin classes can be added but cannot be removed.
    private static List<String> BuiltinClasses = Arrays.asList(
    "com.hazelcast.aggregation.impl.BigDecimalAverageAggregator",
    "com.hazelcast.aggregation.impl.BigDecimalSumAggregator",
    "com.hazelcast.aggregation.impl.BigIntegerAverageAggregator",
    "com.hazelcast.aggregation.impl.BigIntegerSumAggregator",
    "com.hazelcast.aggregation.impl.CountAggregator",
    "com.hazelcast.aggregation.impl.DistinctValuesAggregator",
    "com.hazelcast.aggregation.impl.DoubleAverageAggregator",
    "com.hazelcast.aggregation.impl.DoubleSumAggregator",
    "com.hazelcast.aggregation.impl.FixedSumAggregator",
    "com.hazelcast.aggregation.impl.FloatingPointSumAggregator",
    "com.hazelcast.aggregation.impl.IntegerAverageAggregator",
    "com.hazelcast.aggregation.impl.IntegerSumAggregator",
    "com.hazelcast.aggregation.impl.LongAverageAggregator",
    "com.hazelcast.aggregation.impl.LongSumAggregator",
    "com.hazelcast.aggregation.impl.MaxAggregator",
    "com.hazelcast.aggregation.impl.MinAggregator",
    "com.hazelcast.aggregation.impl.NumberAverageAggregator",

    "com.hazelcast.projection.impl.SingleAttributeProjection",
    "com.hazelcast.projection.impl.MultiAttributeProjection",

    "com.hazelcast.query.SqlPredicate",
    "com.hazelcast.query.impl.predicates.AndPredicate",
    "com.hazelcast.query.impl.predicates.BetweenPredicate",
    "com.hazelcast.query.impl.predicates.EqualPredicate",
    "com.hazelcast.query.impl.predicates.GreaterLessPredicate",
    "com.hazelcast.query.impl.predicates.LikePredicate",
    "com.hazelcast.query.impl.predicates.ILikePredicate",
    "com.hazelcast.query.impl.predicates.InPredicate",
    "com.hazelcast.query.impl.predicates.InstanceOfPredicate",
    "com.hazelcast.query.impl.predicates.NotEqualPredicate",
    "com.hazelcast.query.impl.predicates.NotPredicate",
    "com.hazelcast.query.impl.predicates.OrPredicate",
    "com.hazelcast.query.impl.predicates.RegexPredicate",
    "com.hazelcast.query.impl.FalsePredicate",
    "com.hazelcast.query.TruePredicate",
    "com.hazelcast.query.PagingPredicate",
    "com.hazelcast.query.PartitionPredicate",
    "com.hazelcast.query.PredicateBuilder",

    "com.hazelcast.config.CacheConfig",
    "com.hazelcast.config.CacheConfigReadOnly",
    "com.hazelcast.config.CacheEvictionConfig",
    "com.hazelcast.config.CacheEvictionConfigReadOnly",
    "com.hazelcast.config.WanReplicationRef",
    "com.hazelcast.config.WanReplicationRefReadOnly",
    "com.hazelcast.config.CachePartitionLostListenerConfig",
    "com.hazelcast.config.CachePartitionLostListenerConfigReadOnly",
    "com.hazelcast.config.InMemoryFormat",
    "com.hazelcast.config.EvictionConfig",
    "com.hazelcast.config.EvictionConfigReadOnly",
    "com.hazelcast.config.EvictionConfig$MaxSizePolicy",
    "com.hazelcast.config.EvictionPolicy",

    "com.hazelcast.config.LegacyCacheConfig",
    "com.hazelcast.config.LegacyCacheEvictionConfig",

    "com.hazelcast.cache.HazelcastExpiryPolicy",

    "com.hazelcast.security.UsernamePasswordCredentials",
    "com.hazelcast.topic.impl.reliable.ReliableTopicMessage",
    "com.hazelcast.transaction.impl.xa.SerializableXID",
    "com.hazelcast.executor.impl.RunnableAdapter",
    "com.hazelcast.core.PartitionAwareKey"
    );

    //NEVER CHANGE THESE THREE LIST. CLIENT COMPATIBILITY TESTS WILL BE BROKEN!!!
    //These enum values shouldn't add or remove new enum values.
    private static List<String> MaxSizePolicyNames = Arrays.asList("ENTRY_COUNT","USED_NATIVE_MEMORY_SIZE",
            "USED_NATIVE_MEMORY_PERCENTAGE","FREE_NATIVE_MEMORY_SIZE","FREE_NATIVE_MEMORY_PERCENTAGE");
    private static List<String> EvictionPolicyNames = Arrays.asList("LRU","LFU","NONE","RANDOM");
    private static List<String> InMemoryFormatNames = Arrays.asList("BINARY","OBJECT","NATIVE");


    @Test
    public void BinaryInterfaceListTest() {
        Set<Class<?>> allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(BinaryInterface.class, false);
        for (Class clazz : allAnnotatedClasses) {
            assertTrue("Annotated class is not in the list: " + clazz.getName(), BuiltinClasses.contains(clazz.getName()));
        }
        List<String> list = new ArrayList<String>(BuiltinClasses);
        for(Class clazz : allAnnotatedClasses) {
            list.remove(clazz.getName());
        }
        for(String clazz:list){
            fail("Class should be BinaryInterface annotated: " + clazz);
        }
        assertEquals(BuiltinClasses.size(), allAnnotatedClasses.size());
    }

    @Test
    public void MaxSizePolicyEnumTest() {
        EnumTest(EvictionConfig.MaxSizePolicy.values(), MaxSizePolicyNames);
    }

    @Test
    public void EvictionPolicyEnumTest() {
        EnumTest(EvictionPolicy.values(), EvictionPolicyNames);
    }

    @Test
    public void InMemoryFormatEnumTest() {
        EnumTest(InMemoryFormat.values(), InMemoryFormatNames);
    }

    private <E extends Enum> void EnumTest(E[] values, List<String> names) {
        assertEquals(values.length, names.size());
        for (E enumVal : values) {
            assertTrue(names.contains(enumVal.name()));
        }
    }
}
