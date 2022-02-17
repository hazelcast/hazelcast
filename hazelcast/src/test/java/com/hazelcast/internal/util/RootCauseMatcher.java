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

package com.hazelcast.internal.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for {@link org.junit.rules.ExpectedException#expectCause(Matcher)} to assert the root cause of an exception.
 * <p>
 * Optionally the exception message can be tested as well.
 * <p>
 * Example usage:
 * <pre><code>
 *  {@literal @}Rule
 *   public ExpectedException expect = ExpectedException.none();
 *
 *  {@literal @}Test
 *   public void testRootCause() {
 *     expected.expect(ExecutionException.class);
 *     expected.expectCause(new RootCauseMatcher(StaleTaskException.class));
 *     throwException();
 *   }
 *
 *  {@literal @}Test
 *   public void testRootCause_withMessage() {
 *     expected.expect(ExecutionException.class);
 *     expected.expectCause(new RootCauseMatcher(IllegalStateException.class, "Expected message"));
 *     throwException();
 *   }
 * </code></pre>
 */
public class RootCauseMatcher extends TypeSafeMatcher<Throwable> {

    private final Class<? extends Throwable> expectedType;
    private final String expectedMessage;

    public RootCauseMatcher(Class<? extends Throwable> expectedType) {
        this(expectedType, null);
    }

    public RootCauseMatcher(Class<? extends Throwable> expectedType, String expectedMessage) {
        this.expectedType = expectedType;
        this.expectedMessage = expectedMessage;
    }

    @Override
    protected boolean matchesSafely(Throwable item) {
        item = getRootCause(item);
        if (expectedMessage == null) {
            return item.getClass().isAssignableFrom(expectedType);
        }
        return item.getClass().isAssignableFrom(expectedType) && item.getMessage().contains(expectedMessage);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("expects type ").appendValue(expectedType);
        if (expectedMessage != null) {
            description.appendText(" with message ").appendValue(expectedMessage);
        }
    }

    @Override
    protected void describeMismatchSafely(Throwable item, Description mismatchDescription) {
        super.describeMismatchSafely(getRootCause(item), mismatchDescription);
    }

    public static Throwable getRootCause(Throwable item) {
        while (item.getCause() != null) {
            item = item.getCause();
        }
        return item;
    }
}
