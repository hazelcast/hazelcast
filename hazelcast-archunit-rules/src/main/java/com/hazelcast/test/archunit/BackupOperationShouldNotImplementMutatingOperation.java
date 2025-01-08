/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

public class BackupOperationShouldNotImplementMutatingOperation extends ArchCondition<JavaClass> {
    private static final String MUTATING_OPERATION = "com.hazelcast.spi.impl.operationservice.MutatingOperation";

    BackupOperationShouldNotImplementMutatingOperation() {
        super("not implement " + MUTATING_OPERATION + " interface");
    }

    @Override
    public void check(JavaClass clazz, ConditionEvents events) {
        var implementedInterfaces = clazz.getAllRawInterfaces();
        if (implementedInterfaces.stream().anyMatch(i -> i.getName().equals(MUTATING_OPERATION))) {
            events.add(SimpleConditionEvent.violated(clazz, "Backup operation class " + clazz.getName()
                    + " should not implement MutatingOperation interface"));
        }
    }

    static BackupOperationShouldNotImplementMutatingOperation notImplementMutatingOperation() {
        return new BackupOperationShouldNotImplementMutatingOperation();
    }
}
