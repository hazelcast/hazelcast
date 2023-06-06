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

package com.hazelcast.internal.util;

import org.assertj.core.api.Condition;

/**
 * Matcher for {@link org.assertj.core.api.Assertions#assertThatThrownBy} to assert the root cause of an exception.
 * <p>
 * Optionally the exception message can be tested as well.
 * <p>
 * Example usage:
 * <pre><code>
 *  {@literal @}Test
 *   public void testRootCause() {
 *     assertThatThrownBy(() -> throwException()).cause().has(rootCauseStaleTaskException.class());
 *   }
 *
 *  {@literal @}Test
 *   public void testRootCause_withMessage() {
 *     assertThatThrownBy(() -> throwException()).cause().has(rootCauseStaleTaskException.class(), "Expected message");
 *   }
 * </code></pre>
 */
public class RootCauseMatcher {

    private final Class<? extends Throwable> expectedType;
    private final String expectedMessage;

    public RootCauseMatcher(Class<? extends Throwable> expectedType) {
        this(expectedType, null);
    }

    public RootCauseMatcher(Class<? extends Throwable> expectedType, String expectedMessage) {
        this.expectedType = expectedType;
        this.expectedMessage = expectedMessage;
    }

    public static Condition<Throwable> rootCause(Class<? extends Throwable> expectedType, String expectedMessage) {
        var matcher = new RootCauseMatcher(expectedType, expectedMessage);
        return new Condition<>(matcher::matches, expectedMessage);
    }

    public static Condition<Throwable> rootCause(Class<? extends Throwable> expectedType) {
        return rootCause(expectedType, null);
    }

    private boolean matches(Throwable item) {
        item = getRootCause(item);
        if (expectedMessage == null) {
            return item.getClass().isAssignableFrom(expectedType);
        }
        return item.getClass().isAssignableFrom(expectedType) && item.getMessage().contains(expectedMessage);
    }

    public static Throwable getRootCause(Throwable item) {
        while (item.getCause() != null) {
            item = item.getCause();
        }
        return item;
    }
}
