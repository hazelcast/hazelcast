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

package com.hazelcast.test;

import org.junit.rules.TestRule;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a test rule requirement annotation that connects it with its {@link TestRule} implementation
 * and registers its rule implementation automatically to be executed for filtering.
 */
@Retention(RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface AutoRegisteredTestRule {

    /**
     * Returns class of connected {@link TestRule} implementation
     * for this test rule requirement annotation.
     *
     * @return the class of connected {@link TestRule} implementation
     */
    Class<? extends TestRule> testRule();
}
