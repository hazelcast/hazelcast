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
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.lang.annotation.Annotation;
import java.util.Set;

public class MixTestAnnotationsCondition extends ArchCondition<JavaClass> {
    private static final Set<Class<? extends Annotation>> JUNIT_4_ANNOTATION_CLASSES = Set.of(
            Test.class,
            Before.class,
            After.class,
            BeforeClass.class,
            AfterClass.class
    );

    private static final Set<Class<? extends Annotation>> JUNIT_4_ClASS_ANNOTATION = Set.of(
            Category.class
    );

    private static final Set<Class<? extends Annotation>> JUNIT_5_CLASS_ANNOTATION = Set.of(
            org.junit.jupiter.api.Tag.class
    );
    private static final Set<Class<? extends Annotation>> JUNIT_5_ANNOTATION_CLASSES = Set.of(
            org.junit.jupiter.api.Test.class,
            org.junit.jupiter.api.BeforeEach.class,
            org.junit.jupiter.api.AfterEach.class,
            org.junit.jupiter.api.BeforeAll.class,
            org.junit.jupiter.api.AfterAll.class,
            org.junit.jupiter.api.Tag.class
    );

    public MixTestAnnotationsCondition() {
        super("Do not mix Junit4 and Junit5 annotations");
    }


    @Override
    public void check(JavaClass item, ConditionEvents events) {
        boolean hasJUnit4Elements = hasAnyJUnit4Annotations(item) || hasJUnit4Assertions(item);
        boolean hasJUnit5Elements = hasAnyJUnit5Annotations(item) || hasJUnit5Assertions(item);

        if (hasJUnit4Elements && hasJUnit5Elements) {
            String message = String.format("Class %s mixes JUnit 4 and JUnit 5 elements.", item.getName());
            events.add(SimpleConditionEvent.violated(item, message));
        }
    }

    private boolean hasJUnit5Assertions(JavaClass item) {
        Set<JavaMethodCall> methodCalls = item.getMethodCallsFromSelf();
        return methodCalls.stream().anyMatch(call -> call.getTarget().getFullName().contains("org.junit.jupiter.api.Assertions"));
    }
    private boolean hasJUnit4Assertions(JavaClass item) {
        Set<JavaMethodCall> methodCalls = item.getMethodCallsFromSelf();
        return methodCalls.stream().anyMatch(call -> call.getTarget().getFullName().contains("org.junit.Assert"));
    }

    private boolean hasAnyJUnit5Annotations(JavaClass item) {
        boolean hasJUnit5ClassAnnotation = item.getAnnotations().stream()
                .anyMatch(annotation -> JUNIT_5_CLASS_ANNOTATION.stream()
                        .anyMatch(aa -> aa.isAssignableFrom(annotation.getClass())));

        boolean hasJUnit5Annotation = item.getMethods().stream()
                .anyMatch(method -> JUNIT_5_ANNOTATION_CLASSES.stream()
                        .anyMatch(method::isAnnotatedWith));

        return hasJUnit5Annotation || hasJUnit5ClassAnnotation;
    }
    private boolean hasAnyJUnit4Annotations(JavaClass item) {
        boolean hasJUnit4Annotation = item.getMethods().stream()
                .anyMatch(method -> JUNIT_4_ANNOTATION_CLASSES.stream()
                        .anyMatch(method::isAnnotatedWith));

        boolean hasJUnit4ClassAnnotation = item.getAnnotations().stream()
                .anyMatch(annotation -> JUNIT_4_ClASS_ANNOTATION.stream()
                        .anyMatch(aa -> annotation.getRawType().isAssignableFrom(aa)));

        return hasJUnit4Annotation || hasJUnit4ClassAnnotation;
    }

    public static ArchCondition<JavaClass> notMixJUnit4AndJUnit5Annotations() {
        return new MixTestAnnotationsCondition();
    }
}
