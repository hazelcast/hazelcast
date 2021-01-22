/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.AbstractCaseOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastOperandTypeCheckerAware;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;

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
            boolean valid = operator instanceof HazelcastOperandTypeCheckerAware;

            assertTrue("Operator must implement one of classes from " + HazelcastFunction.class.getPackage().toString()
                    + ": " + operator.getClass().getSimpleName(), valid);
        }
    }

    @Test
    public void testReturnTypeInference() {
        for (SqlOperator operator : HazelcastSqlOperatorTable.instance().getOperatorList()) {
            /**
             * HazelcastInPredicate inherits from Calcite's SqlInOperator due to hacks inside SqlToRelConverter
             * @see org.apache.calcite.sql2rel.SqlToRelConverter##substituteSubQuery
             * */
            if (operator.getKind() == SqlKind.IN || operator.getKind() == SqlKind.NOT_IN) {
                continue;
            }
            boolean valid = operator.getReturnTypeInference() instanceof HazelcastReturnTypeInference;

            assertTrue("Operator must have " + HazelcastReturnTypeInference.class.getSimpleName() + ": " + operator.getClass().getSimpleName(), valid);
        }
    }
}

