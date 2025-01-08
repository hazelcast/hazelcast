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
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/** Asserts that tests are annotated with `@RunWith` to ensure property isolation */
public class TestsHaveRunnersCondition extends ArchCondition<JavaClass> {
    private static final Collection<String> SYSTEM_PROPERTY_MODIFICATION_METHODS =
            Set.of("java.lang.System.setProperty(java.lang.String, java.lang.String)",
                    "com.hazelcast.spi.properties.HazelcastProperty.setSystemProperty(java.lang.String)");

    public TestsHaveRunnersCondition() {
        super("All tests should have @RunWith annotation");
    }

    @Override
    public void check(JavaClass clazz, ConditionEvents events) {
        if (isMutatingSystemProperties(clazz) && !isPropertyIsolated(clazz)) {
            String message = String.format("Class %s modified system properties, but is not isolated with @RunWith annotation",
                    clazz.getName());
            events.add(SimpleConditionEvent.violated(clazz, message));
        }
    }

    private static boolean isPropertyIsolated(JavaClass clazz) {
        // Recursively walk the class hierarchy
        for (JavaClass classToTest = clazz; classToTest.getSuperclass()
                .isPresent(); classToTest = classToTest.getSuperclass()
                        .orElseThrow()
                        .toErasure()) {
            // Check if class is:
            // Annotated with "RunWith"/"UseParametersRunnerFactory"
            if (classToTest.isAnnotatedWith(RunWith.class) || classToTest.isAnnotatedWith(UseParametersRunnerFactory.class)) {
                return true;
            }

            // Has "ParallelJVMTest" tag
            if ((classToTest.isAnnotatedWith(Tag.class) && Objects.equals(classToTest.getAnnotationOfType(Tag.class)
                    .value(), "com.hazelcast.test.annotation.ParallelJVMTest"))
                    || (classToTest.isAnnotatedWith(Category.class) && Arrays
                            .stream(classToTest.getAnnotationOfType(Category.class)
                                    .value())
                            .map(Class::getName)
                            .anyMatch("com.hazelcast.test.annotation.ParallelJVMTest"::equals))) {
                return true;
            }

            // Uses JUnit Pioneer's SetSystemProperty/RestoreSystemProperties
            if (clazz.getAllMethods()
                    .stream()
                    .anyMatch(method -> Stream
                            .of("org.junitpioneer.jupiter.SetSystemProperty",
                                    "org.junitpioneer.jupiter.RestoreSystemProperties")
                            .map(method::tryGetAnnotationOfType)
                            .anyMatch(Optional::isPresent))) {
                return true;
            }
        }

        return false;
    }

    private static boolean isMutatingSystemProperties(JavaClass clazz) {
        return clazz.getAllMethods()
                .stream()
                .map(JavaCodeUnit::getMethodCallsFromSelf)
                .flatMap(Collection::stream)
                .map(JavaMethodCall::getTarget)
                .map(MethodCallTarget::getFullName)
                .anyMatch(SYSTEM_PROPERTY_MODIFICATION_METHODS::contains);
    }
}
