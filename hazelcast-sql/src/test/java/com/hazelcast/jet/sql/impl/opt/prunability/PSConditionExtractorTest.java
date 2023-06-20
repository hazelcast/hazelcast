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

import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.EQUALS;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.junit.Assert.assertEquals;

public class PSConditionExtractorTest {
    PartitionStrategyConditionExtractor extractor;

    @Before
    public void setUp() throws Exception {
        extractor = new PartitionStrategyConditionExtractor();
    }

    @Test
    public void test_singleEquals() {
        HazelcastTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        RexBuilder b = new RexBuilder(typeFactory);
        RexInputRef leftInputRef = b.makeInputRef(typeFactory.createSqlType(INTEGER), 0);
        RexLiteral rexLiteral = b.makeLiteral("1");
        RexCall call = (RexCall) b.makeCall(EQUALS, leftInputRef, rexLiteral);
        List<Tuple3<? extends SqlOperator, RexInputRef, RexNode>> decomposedConds = extractor.extractCondition(call);

        assertEquals(1, decomposedConds.size());
        assertEquals(EQUALS, decomposedConds.get(0).f0());
        assertEquals(leftInputRef, decomposedConds.get(0).f1());
        assertEquals(rexLiteral, decomposedConds.get(0).f2());
    }
}
