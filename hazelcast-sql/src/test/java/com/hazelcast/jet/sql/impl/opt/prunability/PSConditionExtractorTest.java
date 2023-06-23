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

import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.EQUALS;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PSConditionExtractorTest extends OptimizerTestSupport {
    PartitionStrategyConditionExtractor extractor;

    @Before
    public void setUp() throws Exception {
        extractor = new PartitionStrategyConditionExtractor();
    }

    @Test
    public void test_singleEquals() {
        Table table = partitionedTable(
                "m",
                asList(
                        mapField(KEY, INT, QueryPath.KEY_PATH),
                        mapField(VALUE, VARCHAR, QueryPath.VALUE_PATH)),
                10).getTarget();


        HazelcastTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        RexBuilder b = new RexBuilder(typeFactory);
        RexInputRef leftInputRef = b.makeInputRef(typeFactory.createSqlType(INTEGER), 0);
        RexLiteral rexLiteral = b.makeLiteral("1");
        RexCall call = (RexCall) b.makeCall(EQUALS, leftInputRef, rexLiteral);
        List<Tuple4<String, String, RexInputRef, RexNode>> decomposedConds = extractor.extractCondition(
                table, call, Set.of(0));

        assertEquals(1, decomposedConds.size());
        assertEquals("m", decomposedConds.get(0).f0());
        assertEquals("__key", decomposedConds.get(0).f1());
        assertEquals(leftInputRef, decomposedConds.get(0).f2());
        assertEquals(rexLiteral, decomposedConds.get(0).f3());
    }
}
