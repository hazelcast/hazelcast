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

package com.hazelcast.jet.sql.impl.expression.math;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/** This test demonstrates that the MOD function operates identically to the remainder(%) operator. */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ModFunctionIntegrationTest extends RemainderOperatorIntegrationTest {

    @Override
    protected String signatureError(Object type1, Object type2) {
        return signatureErrorFunction("MOD", type1, type2);
    }

    @Override
    protected String sql(Object operand1, Object operand2) {
        return "SELECT MOD(" + operand1 + ", " + operand2 + ") FROM map";
    }
}
