/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.validate.operand;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;

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
