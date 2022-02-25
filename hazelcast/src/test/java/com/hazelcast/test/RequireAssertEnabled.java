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

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Signals that a test method depends on Java assertions being enabled. Typically such
 * a method will expect an {@code AssertionError} to be thrown.
 * <p>
 * To have this annotation honored, a test class must define an instance of {@link AssertEnabledFilterRule}.
 * This is automatically done via {@link AutoRegisteredTestRule}.
 */
@Retention(RUNTIME)
@AutoRegisteredTestRule(testRule = AssertEnabledFilterRule.class)
public @interface RequireAssertEnabled {
}
