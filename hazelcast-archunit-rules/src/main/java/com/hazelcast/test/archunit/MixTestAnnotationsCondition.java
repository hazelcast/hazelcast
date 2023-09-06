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
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

public class MixTestAnnotationsCondition extends ArchCondition<JavaClass> {
    public MixTestAnnotationsCondition() {
        super("Do not mix Junit4 and Junit5 annotations");
    }

    @Override
    public void check(JavaClass item, ConditionEvents events) {
        boolean hasJUnit4Annotation = item.getMethods().stream()
                .anyMatch(method -> method.isAnnotatedWith(org.junit.Test.class)
                                    || method.isAnnotatedWith(org.junit.Before.class)
                                    || method.isAnnotatedWith(org.junit.After.class));

        boolean hasJUnit5Annotation = item.getMethods().stream()
                .anyMatch(method ->
                        method.isAnnotatedWith(org.junit.jupiter.api.Test.class)
                        || method.isAnnotatedWith(org.junit.jupiter.api.BeforeEach.class)
                        || method.isAnnotatedWith(org.junit.jupiter.api.AfterEach.class));

        if (hasJUnit4Annotation && hasJUnit5Annotation) {
            String message = String.format("Class %s mixes JUnit 4 and JUnit 5 annotations.", item.getName());
            events.add(SimpleConditionEvent.violated(item, message));
        }
    }

    public static ArchCondition<JavaClass> notMixJUnit4AndJUnit5Annotations() {
        return new MixTestAnnotationsCondition();
    }
}
