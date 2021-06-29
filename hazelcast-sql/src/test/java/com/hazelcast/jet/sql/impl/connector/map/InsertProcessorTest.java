/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InsertProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private Map<Object, Object> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Test
    public void test_insert() {
        executeInsert(singletonList(new Object[]{1, 1}), singletonList(new Object[]{1L}));
        assertThat(map).containsExactly(entry(1, 1));
    }

    @Test
    public void test_multiInsert() {
        executeInsert(asList(new Object[]{1, 1}, new Object[]{2, 2}), singletonList(new Object[]{2L}));
        assertThat(map).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 1, 2, 2));
    }

    @Test
    public void test_noInput() {
        executeInsert(emptyList(), singletonList(new Object[]{0L}));
        assertThat(map).isEmpty();
    }

    @Test
    public void when_keyIsNull_then_fail() {
        assertThatThrownBy(() -> executeInsert(singletonList(new Object[]{null, 1}), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Key cannot be null");
    }

    @Test
    public void when_keyAlreadyExists_then_fail() {
        assertThatThrownBy(() -> executeInsert(() -> map.put(1, 1), singletonList(new Object[]{1, 2}), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_attemptsToInsertDuplicateKey_then_fail() {
        assertThatThrownBy(() -> executeInsert(asList(new Object[]{1, 1}, new Object[]{1, 2}), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_maxAccumulatedKeysCountIsExceeded_then_fail() {
        assertThatThrownBy(() -> executeInsert(1, asList(new Object[]{1, 1}, new Object[]{2, 2}), emptyList()))
                .isInstanceOf(AccumulationLimitExceededException.class);
    }

    private void executeInsert(List<Object[]> rows, List<Object[]> expectedOutput) {
        executeInsert(() -> map.clear(), Long.MAX_VALUE, rows, expectedOutput);
    }

    private void executeInsert(long maxAccumulatedKeys, List<Object[]> rows, List<Object[]> expectedOutput) {
        executeInsert(() -> map.clear(), maxAccumulatedKeys, rows, expectedOutput);
    }

    private void executeInsert(Runnable setup, List<Object[]> rows, List<Object[]> expectedOutput) {
        executeInsert(setup, Long.MAX_VALUE, rows, expectedOutput);
    }

    private void executeInsert(Runnable setup, long maxAccumulatedKeys, List<Object[]> rows, List<Object[]> expectedOutput) {
        KvProjector.Supplier projectorSupplier = KvProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{INT, INT},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE
        );

        InsertProcessorSupplier processor = new InsertProcessorSupplier(MAP_NAME, projectorSupplier);

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList())
                .setMaxProcessorAccumulatedRecords(maxAccumulatedKeys);
        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .hazelcastInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .executeBeforeEachRun(setup)
                .input(rows)
                .outputChecker(SqlTestSupport::compareRowLists)
                .expectOutput(expectedOutput);
    }
}
