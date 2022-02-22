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
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Sets or clears a property before running a test. The property will be restored once the test is finished.
 * <p>
 * Can be used for finer control of the scope of a System property.
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
        return value == null ? System.clearProperty(propertyName) : System.setProperty(propertyName, value);
    }
}
