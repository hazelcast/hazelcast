/*
 * Copyright 2025 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunction;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastOperandTypeCheckerAware;
import com.hazelcast.jet.sql.impl.validate.operators.misc.HazelcastCaseOperator;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastSqlOperatorTableTest {
    /**
     * Make sure there are no overrides for operators defined in the operator table.
     */
    @Test
    public void testNoOverride() {
        Map<BiTuple<String, SqlSyntax>, SqlOperator> map = new HashMap<>();

        for (SqlOperator operator : HazelcastSqlOperatorTable.instance().getOperatorList()) {
            BiTuple<String, SqlSyntax> key = BiTuple.of(operator.getName(), operator.getSyntax());

            SqlOperator oldOperator = map.put(key, operator);

            assertNull("Duplicate operator \"" + operator.getName(), oldOperator);
        }
    }

    /**
     * Make sure that all our operators either define the top-level operand checker that overrides that call binding,
     * or confirm explicitly that they override the binding manually.
     */
    @Test
    public void testOperandTypeChecker() {
        for (SqlOperator operator : HazelcastSqlOperatorTable.instance().getOperatorList()) {
            boolean valid = operator instanceof HazelcastOperandTypeCheckerAware
                    || operator instanceof HazelcastTableFunction
                    || operator instanceof HazelcastCaseOperator
                    || operator == HazelcastSqlOperatorTable.ARGUMENT_ASSIGNMENT
                    || operator == HazelcastSqlOperatorTable.DOT;

            assertTrue("Operator must implement one of classes from " + HazelcastFunction.class.getPackage().toString()
                    + ": " + operator.getClass().getSimpleName(), valid);
        }
    }

    @Test
    public void testReturnTypeInference() {
        for (SqlOperator operator : HazelcastSqlOperatorTable.instance().getOperatorList()) {
            if (operator instanceof HazelcastTableFunction
                    || operator == HazelcastSqlOperatorTable.IN
                    || operator == HazelcastSqlOperatorTable.NOT_IN
                    || operator == HazelcastSqlOperatorTable.UNION
                    || operator == HazelcastSqlOperatorTable.UNION_ALL
                    || operator == HazelcastSqlOperatorTable.ARGUMENT_ASSIGNMENT
                    || operator == HazelcastSqlOperatorTable.DOT) {
                continue;
            }
            boolean valid = operator.getReturnTypeInference() instanceof HazelcastReturnTypeInference;

            assertTrue("Operator must have " + HazelcastReturnTypeInference.class.getSimpleName() + ": " + operator.getClass().getSimpleName(), valid);
        }
    }
}
