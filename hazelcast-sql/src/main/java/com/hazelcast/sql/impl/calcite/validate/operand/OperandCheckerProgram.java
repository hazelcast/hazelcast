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

/**
 * Composite program that invokes a separate operand checker for every operand.
 */
public class OperandCheckerProgram {

    private final OperandChecker[] checkers;

    public OperandCheckerProgram(OperandChecker... checkers) {
        this.checkers = checkers;
    }

    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        boolean res = true;

        for (int i = 0; i < checkers.length; i++) {
            boolean checkerRes = checkers[i].check(callBinding, false, i);

            if (!checkerRes) {
                res = false;
            }
        }

        if (!res && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return res;
    }
}
