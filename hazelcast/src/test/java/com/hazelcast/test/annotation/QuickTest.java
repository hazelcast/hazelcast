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

package com.hazelcast.test.annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;

import java.time.Duration;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotates quick tests which are fast enough (i.e. execution sub-{@link #EXPECTED_RUNTIME_THRESHOLD} per test) for the PR
 * builder.
 * <p>
 * Will be executed in the PR builder and for code coverage measurements.
 *
 * @see SlowTest
 */
@Retention(RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Tag("com.hazelcast.test.annotation.QuickTest")
public @interface QuickTest {
    Duration EXPECTED_RUNTIME_THRESHOLD = Duration.ofMinutes(1);
}
