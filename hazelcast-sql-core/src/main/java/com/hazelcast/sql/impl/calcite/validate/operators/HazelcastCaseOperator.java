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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.common.AbstractHazelcastCaseOperator;

public class HazelcastCaseOperator extends AbstractHazelcastCaseOperator {

    public static final HazelcastCaseOperator INSTANCE = new HazelcastCaseOperator();

    private HazelcastCaseOperator() {
        super();
    }

    @Override
    protected final boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
//        return SqlCaseOperator.INSTANCE.checkOperandTypes(binding, throwOnFailure);
//        if (binding instanceof SqlCallBinding) {
//            SqlCallBinding sqlBinding = (SqlCallBinding) binding;
//            SqlCase call = (SqlCase) sqlBinding.getCall();
//            HazelcastSqlValidator validator = (HazelcastSqlValidator) sqlBinding.getValidator();
//
//            validator.getTypeCoercion().caseWhenCoercion(sqlBinding);
//            RelDataType caseType = validator.getKnownNodeType(call);
//            if (caseType == null) {
//                throw sqlBinding.newValidationError(RESOURCE.dynamicParamIllegal());
//            }
//
//            for (SqlNode thenOperand : call.getThenOperands()) {
//                RelDataType thenOperandType = validator.deriveType(sqlBinding.getScope(), thenOperand);
//                if (!HazelcastTypeSystem.canCast(thenOperandType, caseType)) {
//                    throw sqlBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
//                }
//            }
//            SqlNode elseOperand = call.getElseOperand();
//            RelDataType elseOperandType = validator.deriveType(sqlBinding.getScope(), elseOperand);
//            if (!HazelcastTypeSystem.canCast(elseOperandType, caseType)) {
//                throw sqlBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
//            }
//
//            return caseType;
//        } else {
//            return SqlCaseOperator.INSTANCE.inferReturnType(binding);
//        }
        return false;
    }
}
