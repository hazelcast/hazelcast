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

package com.hazelcast.sql.impl.calcite.validate.operand;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ARGUMENT_ASSIGNMENT;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;

@RunWith(MockitoJUnitRunner.class)
public class OperandCheckerProgramTest {

    private OperandCheckerProgram program;

    @Mock
    private SqlValidator validator;

    @Mock
    private SqlFunction function;

    @Mock
    private OperandChecker firstOperandChecker;

    @Mock
    private OperandChecker secondOperandChecker;

    @Before
    public void setUp() {
        program = new OperandCheckerProgram(firstOperandChecker, secondOperandChecker);
    }

    @Test
    public void test_check() {
        SqlCallBinding sqlCallBinding =
                new SqlCallBinding(validator, null, new SqlBasicCall(function, new SqlNode[0], ZERO));
        HazelcastCallBinding hazelcastBinding = new HazelcastCallBinding(sqlCallBinding);

        program.check(hazelcastBinding, false);

        InOrder inOrder = inOrder(firstOperandChecker, secondOperandChecker);
        inOrder.verify(firstOperandChecker).check(hazelcastBinding, false, 0);
        inOrder.verify(secondOperandChecker).check(hazelcastBinding, false, 1);
    }

    @Test
    public void when_namedParametersAreUsed_then_rightCheckersAreApplied() {
        SqlNode[] operands = new SqlNode[]{
                new SqlBasicCall(
                        ARGUMENT_ASSIGNMENT,
                        new SqlNode[]{SqlLiteral.createCharString("2", ZERO), new SqlIdentifier("second", ZERO)},
                        ZERO
                ),
                new SqlBasicCall(
                        ARGUMENT_ASSIGNMENT,
                        new SqlNode[]{SqlLiteral.createCharString("1", ZERO), new SqlIdentifier("first", ZERO)},
                        ZERO
                )
        };
        SqlCallBinding sqlCallBinding = new SqlCallBinding(validator, null, new SqlBasicCall(function, operands, ZERO));
        HazelcastCallBinding hazelcastBinding = new HazelcastCallBinding(sqlCallBinding);

        given(function.getParamNames()).willReturn(asList("first", "second"));

        program.check(hazelcastBinding, false);

        InOrder inOrder = inOrder(firstOperandChecker, secondOperandChecker);
        inOrder.verify(secondOperandChecker).check(hazelcastBinding, false, 0);
        inOrder.verify(firstOperandChecker).check(hazelcastBinding, false, 1);
    }
}
