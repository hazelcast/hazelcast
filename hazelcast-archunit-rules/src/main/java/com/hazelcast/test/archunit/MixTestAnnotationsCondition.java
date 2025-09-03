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

import com.tngtech.archunit.core.domain.AccessTarget.MethodCallTarget;
import com.tngtech.archunit.core.domain.JavaAnnotation;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MixTestAnnotationsCondition extends ArchCondition<JavaClass> {
    private static final Set<String> JUNIT_PACKAGES = Stream.of(
            // JUnit 3
            junit.framework.TestCase.class,
            // JUnit 4
            org.junit.Test.class,
            // JUnit 5
            org.junit.jupiter.api.Test.class)
            .map(Class::getPackage)
            .map(Package::getName)
            .collect(Collectors.toSet());

    public MixTestAnnotationsCondition() {
        super("Do not mix different JUnit version references in the same Class");
    }

    @Override
    public void check(JavaClass item, ConditionEvents events) {
        Set<String> junitReferences = Stream.concat(getAnnotationsForClass(item), getMethodReferencesInClass(item))
                .map(JavaClass::getPackageName)
                .distinct()
                .filter(JUNIT_PACKAGES::contains)
                .collect(Collectors.toSet());

        // If the class contains multiple _different_ JUnit packages
        if (junitReferences.size() > 1) {
            String message =
                    String.format("Class %s mixes different JUnit version references %s.", item.getName(), junitReferences);
            events.add(SimpleConditionEvent.violated(item, message));
        }
    }

    /** @return a {@link Stream} of the {@link Class}' annotating {@code item} */
    private static Stream<JavaClass> getAnnotationsForClass(JavaClass item) {
        Stream<JavaAnnotation<JavaClass>> classAnnotations = item.getAnnotations()
                .stream();

        Stream<JavaAnnotation<JavaMethod>> methodAnnotations = item.getMethods()
                .stream()
                .map(JavaMethod::getAnnotations)
                .flatMap(Collection::stream);

        return Stream.concat(classAnnotations, methodAnnotations)
                .map(JavaAnnotation::getRawType);
    }

    /** @return a {@link Stream} of {@link Class}' containing methods called from {@code item} */
    private static Stream<JavaClass> getMethodReferencesInClass(JavaClass item) {
        return item.getMethodCallsFromSelf()
                .stream()
                .map(JavaMethodCall::getTarget)
                .map(MethodCallTarget::getOwner);
    }

    public static ArchCondition<JavaClass> notMixDifferentJUnitVersionsAnnotations() {
        return new MixTestAnnotationsCondition();
    }
}
