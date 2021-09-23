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

public final class AnyOperandChecker implements OperandChecker {

    public static final AnyOperandChecker INSTANCE = new AnyOperandChecker();

    private AnyOperandChecker() {
        // No-op
    }

    @Override
    public boolean check(HazelcastCallBinding callBinding, boolean throwOnFailure, int i) {
        return true;
    }
}
