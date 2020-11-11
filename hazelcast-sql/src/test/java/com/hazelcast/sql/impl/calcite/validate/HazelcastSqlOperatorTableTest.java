/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverrideOperandChecker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.fail;

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
    public void testSqlBindingOverride() {
        for (SqlOperator operator : HazelcastSqlOperatorTable.instance().getOperatorList()) {
            SqlOperandTypeChecker operandTypeChecker = operator.getOperandTypeChecker();

            boolean valid = operator instanceof SqlCallBindingManualOverride
                || operandTypeChecker instanceof SqlCallBindingOverrideOperandChecker;

            if (valid) {
                continue;
            }

            fail("Operator \"" + operator.getName() + "\" must have \""
                + SqlCallBindingOverrideOperandChecker.class.getSimpleName() + "\" as top-level operand type checker or "
                + "implement " + SqlCallBindingManualOverride.class.getSimpleName() + " interface"
            );
        }
    }
}
