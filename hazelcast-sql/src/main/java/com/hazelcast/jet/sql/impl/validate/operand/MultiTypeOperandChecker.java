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

import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;

public final class MultiTypeOperandChecker extends TypedOperandChecker {
    public static final TypedOperandChecker JSON_OR_VARCHAR = new MultiTypeOperandChecker(JSON, VARCHAR);

    private final List<TypedOperandChecker> secondaryOperandCheckers;

    private MultiTypeOperandChecker(
            final TypedOperandChecker primaryTypeChecker,
            final TypedOperandChecker ...secondaryOperandCheckers
    ) {
        super(primaryTypeChecker.type);
        if (secondaryOperandCheckers == null || secondaryOperandCheckers.length == 0) {
            throw new IllegalArgumentException("SecondaryOperandCheckers argument can not be empty");
        }
        this.secondaryOperandCheckers = Arrays.asList(secondaryOperandCheckers);

    }

    @Override
    protected boolean matchesTargetType(final RelDataType operandType) {
        return super.matchesTargetType(operandType) || secondaryOperandCheckers.stream()
                .anyMatch(typedOperandChecker -> typedOperandChecker.matchesTargetType(operandType));
    }
}
