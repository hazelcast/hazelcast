/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.annotation;

import com.hazelcast.test.AutoRegisteredTestRule;
import com.hazelcast.test.starter.IgnoreCompatibilityTestsWithSinceRule;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Indicate that a compatibility test is meant to be executed once the current codebase version is at least
 * the one indicated in {@link #value()}.
 */
@Retention(RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@AutoRegisteredTestRule(testRule = IgnoreCompatibilityTestsWithSinceRule.class)
public @interface TestForCompatibilitySince {
    /**
     * @return codebase version since which the annotated test will be executed as a compatibility test
     */
    String value();
}
