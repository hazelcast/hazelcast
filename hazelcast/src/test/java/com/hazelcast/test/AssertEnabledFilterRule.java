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
 * Prevents a test annotated with {@link RequireAssertEnabled} from running when Java assertions are disabled.
 * <p>
 * Typically such a test will expect an {@code AssertionError} to be thrown.
 * <p>
 * <b>Note:</b> This rule is automatically registered when {@link RequireAssertEnabled} is used.
 */
public class AssertEnabledFilterRule implements TestRule {

    @Override
    public Statement apply(final Statement base, final Description description) {
        if (description.getAnnotation(RequireAssertEnabled.class) == null) {
            return base;
        }
        return new Statement() {
            @Override
            @SuppressWarnings({"ConstantConditions", "AssertWithSideEffects"})
            public void evaluate() throws Throwable {
                boolean assertEnabled = false;
                assert assertEnabled = true;
                if (!assertEnabled) {
                    throw new AssertionError("Java assertions are not enabled (java -ea ...), but this test requires them: "
                            + description.getDisplayName());
                }
                base.evaluate();
            }
        };
    }
}
