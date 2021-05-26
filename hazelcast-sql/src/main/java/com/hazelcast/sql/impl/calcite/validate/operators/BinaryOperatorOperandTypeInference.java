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

package com.hazelcast.sql.impl.calcite.validate.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;

public final class BinaryOperatorOperandTypeInference
        extends AbstractOperandTypeInference<BinaryOperatorOperandTypeInference.IndexState> {
    public static final BinaryOperatorOperandTypeInference INSTANCE = new BinaryOperatorOperandTypeInference();

    private BinaryOperatorOperandTypeInference() {
    }

    @Override
    protected IndexState createLocalState() {
        return new IndexState();
    }

    @Override
    protected void precondition(RelDataType[] operandTypes, SqlCallBinding binding) {
        assert operandTypes.length == 2;
        assert binding.getOperandCount() == 2;
    }

    @Override
    protected void updateUnresolvedTypes(
            RelDataType knownType,
            RelDataType[] operandTypes,
            IndexState state,
            RelDataTypeFactory typeFactory,
            boolean knownTypeIsIntervalType
    ) {
        assignType(knownType, operandTypes, knownTypeIsIntervalType, state.unknownTypeOperandIndex, typeFactory);
    }

    static class IndexState implements AbstractOperandTypeInference.State {
        int unknownTypeOperandIndex;

        @Override
        public void update(int index) {
            unknownTypeOperandIndex = index;
        }
    }
}
