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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IdentityTest extends ExpressionTestBase {

    @Test
    public void verify() {
        verify(IDENTITY, IdentityTest::expectedTypes, IdentityTest::expectedValues, "%s", ALL);
    }

    private static RelDataType[] expectedTypes(Operand[] operands) {
        Operand operand = operands[0];

        if (operand.isParameter()) {
            return null;
        }

        RelDataType type = operand.type;

        // Assign type to numeric literals.

        if (operand.isNumericLiteral()) {
            Number numeric = operand.numericValue();
            assert numeric != null && numeric != INVALID_NUMERIC_VALUE;
            type = narrowestTypeFor(numeric, null);
        }

        // Validate literals.

        if (operand.isLiteral() && !canRepresentLiteral(operand, type)) {
            return null;
        }

        return new RelDataType[]{type, type};
    }

    private static Object expectedValues(Operand[] operands, RelDataType[] types, Object[] args) {
        return args[0];
    }

}
