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

package com.hazelcast.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Objects;
import java.util.function.Supplier;

public class ClosingRule<T extends AutoCloseable>
        implements TestRule {

    private final Supplier<T> resourceSupplier;
    private T resourceInstance;

    public ClosingRule(Supplier<T> resourceSupplier) {
        this.resourceSupplier = resourceSupplier;
    }

    public T get() {
        return Objects.requireNonNull(resourceInstance);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate()
                    throws Throwable {
                resourceInstance = resourceSupplier.get();
                try {
                    base.evaluate();
                } finally {
                    resourceInstance.close();
                }
            }
        };
    }
}
