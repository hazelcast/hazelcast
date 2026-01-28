/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.test.archunit.TestAnnotationCategoryAndTagCheck.hasJunitTestCategoryOrTagAnnotation;

public class CategoryCompatibilityRule {

    private static final String COMPATIBILITY_CLASS = "com.hazelcast.test.annotation.CompatibilityTest";
    private static final String NIGHTLY_CLASS = "com.hazelcast.test.annotation.NightlyTest";
    private static final String SLOW_CLASS = "com.hazelcast.test.annotation.SlowTest";

    private static final Set<String> FORBIDDEN_WITH_COMPATIBILITY =
            Set.of(NIGHTLY_CLASS, SLOW_CLASS);

    private CategoryCompatibilityRule() {
        // utility class
    }

    static ArchCondition<JavaClass> compatibilityCategoryCompatibilityNotRunningNightlyOrSlow() {
        return new ArchCondition<>("have CompatibilityTest category only alone") {
            @Override
            public void check(JavaClass javaClass, ConditionEvents events) {
                boolean isCompatibilityTest = hasJunitTestCategoryOrTagAnnotation(javaClass, COMPATIBILITY_CLASS);
                if (!isCompatibilityTest) {
                    return;
                }
                Set<String> forbiddenFound =
                        FORBIDDEN_WITH_COMPATIBILITY.stream()
                                .filter(annotation -> hasJunitTestCategoryOrTagAnnotation(javaClass, annotation))
                                .collect(Collectors.toSet());
                if (!forbiddenFound.isEmpty()) {
                    String message = String.format(
                            "%s has @Category CompatibilityTest combined with forbidden categories %s",
                            javaClass.getName(),
                            forbiddenFound
                    );
                    events.add(SimpleConditionEvent.violated(javaClass, message));
                }
            }
        };
    }

}
