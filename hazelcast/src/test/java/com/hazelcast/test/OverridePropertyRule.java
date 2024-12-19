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

package com.hazelcast.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Sets or clears a property before running a test. The property will be restored once the test is finished.
 * <p>
 * Can be used for finer control of the scope of a System property.
 * <p>
 * When migrating to JUnit5 consider using {@link org.junitpioneer.jupiter.SetEnvironmentVariable} and related.
 */
public final class OverridePropertyRule implements TestRule {

    private final String propertyName;
    private final String value;

    private OverridePropertyRule(String propertyName, String value) {
        this.propertyName = propertyName;
        this.value = value;
    }

    /**
     * Clears the property.
     *
     * @param propertyName system property to clear
     * @return instance of the rule
     */
    public static OverridePropertyRule clear(String propertyName) {
        return new OverridePropertyRule(propertyName, null);
    }

    /**
     * Set the property to a {@code newValue}.
     *
     * @param propertyName system property to set
     * @param newValue     value to set
     * @return instance of the rule
     */
    public static OverridePropertyRule set(String propertyName, String newValue) {
        return new OverridePropertyRule(propertyName, newValue);
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                String oldValue = setOrClearProperty(value);
                try {
                    base.evaluate();
                } finally {
                    setOrClearProperty(oldValue);
                }
            }
        };
    }

    public String setOrClearProperty(String value) {
        return setOrClearProperty(propertyName, value);
    }

    public static String setOrClearProperty(String propertyName, String value) {
        return value == null ? System.clearProperty(propertyName) : System.setProperty(propertyName, value);
    }

    /**
     * Executes given piece of code with given system property changed.
     * <b>Note</b> that this method should be used only in {@link HazelcastSerialClassRunner serial tests} as
     * system properties are static and changes will interfere with other tests.
     */
    public static void withChangedProperty(@Nonnull final String propertyName,
                                           @Nonnull final String newValue,
                                           @Nonnull final Runnable action) {
        requireNonNull(propertyName, "property name must be non-null");
        requireNonNull(newValue, "property name must be non-null");
        requireNonNull(action, "property name must be non-null");

        String oldValue = setOrClearProperty(propertyName, newValue);
        try {
            action.run();
        } finally {
            setOrClearProperty(propertyName, oldValue);
        }
    }
}
