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

import java.util.ArrayList;
import java.util.List;

public final class VariableLengthOperandTypeInference
        extends AbstractOperandTypeInference<VariableLengthOperandTypeInference.OperandsIndexState> {
    public static final VariableLengthOperandTypeInference INSTANCE = new VariableLengthOperandTypeInference();

    private VariableLengthOperandTypeInference() {
    }

    @Override
    protected OperandsIndexState createLocalState() {
        return new OperandsIndexState();
    }

    @Override
    protected void precondition(RelDataType[] operandTypes, SqlCallBinding binding) {
    }

    @Override
    protected void updateUnresolvedTypes(
            RelDataType knownType,
            RelDataType[] operandTypes,
            OperandsIndexState state,
            RelDataTypeFactory typeFactory,
            boolean knownTypeIsIntervalType
    ) {
        if (!state.unknownTypeOperandIndexes.isEmpty()) {
            for (int unknownTypeOperandIndex : state.unknownTypeOperandIndexes) {
                assignType(knownType, operandTypes, knownTypeIsIntervalType, unknownTypeOperandIndex, typeFactory);
            }
        }
    }

    static class OperandsIndexState implements State {
        private final List<Integer> unknownTypeOperandIndexes = new ArrayList<>();

        @Override
        public void update(int index) {
            unknownTypeOperandIndexes.add(index);
        }
    }
}
