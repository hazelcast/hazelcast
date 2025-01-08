/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serial;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NoIndexLossAfterSplitBrainHealTest extends SplitBrainTestSupport {
    private static final int ENTRY_COUNT = 10000;
    private static final int[] BRAINS = new int[]{2, 3};
    private static final String MAP_NAME = randomMapName();
    private static final String[] POSSIBLE_VALUES = new String[] {"A", "B", "C", "D"};
    /** Logically equivalent for the input dataset */
    private static final Collection<Predicate<Object, TestObject>> TEST_PREDICATES =
            List.of(Predicates.alwaysTrue(), Predicates.not(Predicates.equal("value", "X")), Predicates.in("value", POSSIBLE_VALUES));

    // Trying to investigate flakey test https://github.com/hazelcast/hazelcast/issues/25725
    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug-map.xml");

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<InMemoryFormat> parameters() {
        return List.of(InMemoryFormat.BINARY, InMemoryFormat.OBJECT);
    }

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected Config config() {
        return super.config().addMapConfig(new MapConfig()
                .setName(MAP_NAME).setInMemoryFormat(inMemoryFormat)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "value")));
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        IMap<Object, TestObject> testMap = instances[0].getMap(MAP_NAME);

        for (long key = 0; key < ENTRY_COUNT; key++) {
            String randomValue = POSSIBLE_VALUES[ThreadLocalRandomProvider.get()
                    .nextInt(POSSIBLE_VALUES.length)];
            testMap.put(key, new TestObject(key, randomValue));
        }

        test(instances);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        test(instances);
    }

    private static void test(HazelcastInstance[] instances) {
        for (HazelcastInstance instance : instances) {
            IMap<Object, TestObject> testMap = instance.getMap(MAP_NAME);

            // com.hazelcast.map.merge.NoIndexLossAfterSplitBrainHealTest.testSplitBrain [HZ-3851] #25725
            // The predicates are returning unexpected values, so iteratively step up through the logic chain to see where
            // exactly it goes wrong

            assertSizeEventually(ENTRY_COUNT, testMap);

            assertEquals("Map size has been modified mid-assertion", ENTRY_COUNT, testMap.size());
            assertThat(testMap.values()).as("The size of \"values()\" and the maps' \"size()\" should be the same")
                    .hasSize(ENTRY_COUNT);
            assertThat(testMap.keySet()).as("The size of \"keySet()\" and the maps' \"size()\" should be the same")
                    .hasSize(ENTRY_COUNT);

            TEST_PREDICATES.forEach(predicate -> assertThat(testMap.keySet(predicate)).as(() -> {
                Predicate<Object, TestObject> inversedPredicate = Predicates.not(predicate);
                Collection<Object> inversedPredicateResult = testMap.keySet(inversedPredicate);

                return MessageFormat.format(
                        """
                                Previous assertion established the size of testMap is {0}.
                                It''s therefore impossible for the predicate "{1}" (which is functionally equivalent to "true") to return anything other than that same result.
                                The inverse of this predicate ("{2}") (which is functionally equivalent to "false") returned {3} results ({4}).
                                """,
                        ENTRY_COUNT, predicate, inversedPredicate, inversedPredicateResult.size(), inversedPredicateResult);
            })
                    .hasSize(ENTRY_COUNT));
        }
    }

    private record TestObject(Long id, String value) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}
