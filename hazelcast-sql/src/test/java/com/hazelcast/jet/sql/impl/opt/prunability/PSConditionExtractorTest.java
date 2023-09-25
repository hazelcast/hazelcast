/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.prunability;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.AND;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.EQUALS;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.OR;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PSConditionExtractorTest extends OptimizerTestSupport {
    private PartitionStrategyConditionExtractor extractor;
    private HazelcastTypeFactory typeFactory;

    @Before
    public void setUp() throws Exception {
        extractor = new PartitionStrategyConditionExtractor();
        typeFactory = HazelcastTypeFactory.INSTANCE;
    }

    @Test
    public void test_singleEquals() {
        var table = partitionedTable(
                "m",
                asList(
                        mapField(KEY, INT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                emptyList(),
                10, emptyList(), true).getTarget();

        var b = new RexBuilder(typeFactory);
        var leftInputRef = b.makeInputRef(typeFactory.createSqlType(INTEGER), 0);
        var rexLiteral = b.makeLiteral("1");
        var call = (RexCall) b.makeCall(EQUALS, leftInputRef, rexLiteral);

        var decomposedConds = extractor.extractCondition(table, call, Set.of(KEY));
        assertEquals(Map.of("m", singletonList(Map.of("__key", rexLiteral))), decomposedConds);
    }

    @Test
    public void test_multiEqualsAndWithCompleteFilter() {
        final PartitionedMapTable table = partitionedTable(
                "m",
                asList(
                        mapField("comp0", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp1")),
                        mapField("comp1", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp2")),
                        mapField("comp2", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp3")),
                        mapField(KEY, OBJECT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                Collections.emptyList(), 10, Arrays.asList("comp1", "comp2"), true).getTarget();

        // comp0 = ?2 AND comp1 = ?1 AND comp2 = ?0
        var b = new RexBuilder(typeFactory);
        var param0 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        var param1 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
        var param2 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 2);
        var col0 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        var col1 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
        var col2 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 2);

        var filter = (RexCall) b.makeCall(AND,
                b.makeCall(EQUALS, col0, param2),
                b.makeCall(EQUALS, col1, param1),
                b.makeCall(EQUALS, col2, param0)
        );

        var decomposedConds = extractor.extractCondition(table, filter, Set.of("comp1", "comp2"));
        assertEquals(Map.of("m", singletonList(Map.of(
                "comp1", param1,
                "comp2", param0
                ))), decomposedConds);
    }

    @Test
    public void whenOrConditionIsPresent_thenReturnNoVariants() {
        final PartitionedMapTable table = partitionedTable(
                "m",
                asList(
                        mapField("comp0", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp1")),
                        mapField("comp1", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp2")),
                        mapField("comp2", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp3")),
                        mapField(KEY, OBJECT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                Collections.emptyList(), 10, Arrays.asList("comp1", "comp2"), true).getTarget();

        // comp0 = ?2 AND comp1 = ?1 AND comp2 = ?0
        var b = new RexBuilder(typeFactory);
        var param0 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        var param1 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
        var param2 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 2);
        var col0 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        var col1 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
        var col2 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 2);

        var filter = (RexCall) b.makeCall(OR,
                b.makeCall(EQUALS, col0, param2),
                b.makeCall(EQUALS, col1, param1),
                b.makeCall(EQUALS, col2, param0)
        );

        var decomposedConds = extractor.extractCondition(table, filter, Set.of("comp1", "comp2"));
        assertEquals(0, decomposedConds.size());
    }

    @Test
    public void whenConditionIsIncomplete_thenReturnNoVariants() {
        final PartitionedMapTable table = partitionedTable(
                "m",
                asList(
                        mapField("comp0", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp1")),
                        mapField("comp1", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp2")),
                        mapField("comp2", BIGINT, QueryPath.create(QueryPath.KEY_PREFIX + "comp3")),
                        mapField(KEY, OBJECT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                Collections.emptyList(), 10, Arrays.asList("comp1", "comp2"), true).getTarget();

        // comp0 = ?2 AND comp1 = ?1 AND comp2 = ?0
        var b = new RexBuilder(typeFactory);
        var param0 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        var param2 = b.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.BIGINT), 2);
        var col0 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 0);
        var col2 = b.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 2);

        var filter = (RexCall) b.makeCall(AND,
                b.makeCall(EQUALS, col0, param2),
                b.makeCall(EQUALS, col2, param0)
        );

        var decomposedConds = extractor.extractCondition(table, filter, Set.of("comp1", "comp2"));
        assertEquals(0, decomposedConds.size());
    }
}
