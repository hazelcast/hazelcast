/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class TestIgnoreRuleAccordingToUnalignedMemoryAccessSupport implements TestRule {

    private static final ILogger LOGGER = Logger.getLogger(TestIgnoreRuleAccordingToUnalignedMemoryAccessSupport.class);

    @Override
    public Statement apply(Statement base, final Description description) {
        if (description.getAnnotation(RequiresUnalignedMemoryAccessSupport.class) != null
                && !AlignmentUtil.isUnalignedAccessAllowed()) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    LOGGER.finest("Ignoring `" + description.getClassName()
                            + "` because unaligned memory access is not supported in this platform");
                }
            };
        }
        return base;
    }
}
