/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Optional;

import static com.tngtech.archunit.lang.conditions.ArchConditions.beFinal;
import static com.tngtech.archunit.lang.conditions.ArchConditions.beStatic;
import static com.tngtech.archunit.lang.conditions.ArchConditions.haveRawType;

public class SerialVersionUidFieldCondition extends ArchCondition<JavaClass> {
    private static final String FIELD_NAME = "serialVersionUID";

    SerialVersionUidFieldCondition() {
        super("have a valid " + FIELD_NAME);
    }

    @Override
    public void check(JavaClass clazz, ConditionEvents events) {
        Optional<JavaField> field = clazz.tryGetField(FIELD_NAME);
        if (field.isPresent()) {
            haveRawType("long").and(beFinal()).and(beStatic()).check(field.get(), events);
        } else {
            events.add(SimpleConditionEvent.violated(clazz, FIELD_NAME + " field is missing in class " + clazz.getName()));
        }
    }

    static SerialVersionUidFieldCondition haveValidSerialVersionUid() {
        return new SerialVersionUidFieldCondition();
    }
}
