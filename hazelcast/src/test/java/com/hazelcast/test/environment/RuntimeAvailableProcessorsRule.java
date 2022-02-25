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

package com.hazelcast.test.environment;

import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule for overriding number of available processors on the system for a given test.
 */
public class RuntimeAvailableProcessorsRule implements TestRule {

    private final int count;

    public RuntimeAvailableProcessorsRule(int count) {
        this.count = count;
    }

    @Override
    public Statement apply(final Statement statement, Description description) {
        return new Statement() {
            @Override
            public void evaluate()
                    throws Throwable {
                try {
                    RuntimeAvailableProcessors.override(count);
                    statement.evaluate();
                } finally {
                    RuntimeAvailableProcessors.resetOverride();
                }
            }
        };
    }

}
