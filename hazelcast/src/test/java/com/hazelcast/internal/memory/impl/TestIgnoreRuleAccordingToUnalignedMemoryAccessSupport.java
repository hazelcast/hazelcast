/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory.impl;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.assertj.core.api.Assumptions.assumeThat;

public class TestIgnoreRuleAccordingToUnalignedMemoryAccessSupport implements TestRule {

    @Override
    public Statement apply(Statement base, final Description description) {
        if (description.getAnnotation(RequiresUnalignedMemoryAccessSupport.class) != null) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    assumeThat(AlignmentUtil.isUnalignedAccessAllowed())
                            .as("Unaligned memory access is not supported in this platform")
                            .isTrue();
                    base.evaluate();
                }
            };
        }
        return base;
    }
}
