/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

public class OperationShouldNotImplementReadonlyAndMutatingOperation extends ArchCondition<JavaClass> {
    private static final String READONLY_OPERATION = "com.hazelcast.spi.impl.operationservice.ReadonlyOperation";
    private static final String MUTATING_OPERATION = "com.hazelcast.spi.impl.operationservice.MutatingOperation";

    OperationShouldNotImplementReadonlyAndMutatingOperation() {
        super("not implement both " + READONLY_OPERATION + " and " + MUTATING_OPERATION + " interfaces");
    }

    @Override
    public void check(JavaClass clazz, ConditionEvents events) {
        var implementedInterfaces = clazz.getAllRawInterfaces();
        var implementsReadonly = implementedInterfaces.stream().anyMatch(i -> i.getName().equals(READONLY_OPERATION));
        var implementsMutatingOp = implementedInterfaces.stream().anyMatch(i -> i.getName().equals(MUTATING_OPERATION));
        if (implementsReadonly && implementsMutatingOp) {
            events.add(SimpleConditionEvent.violated(clazz, "Operation class " + clazz.getName()
                    + " should not implement both ReadonlyOperation and MutatingOperation interfaces"));
        }
    }

    static OperationShouldNotImplementReadonlyAndMutatingOperation notImplementReadonlyAndMutatingOperation() {
        return new OperationShouldNotImplementReadonlyAndMutatingOperation();
    }
}
