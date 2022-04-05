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

package com.hazelcast.test.starter;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.TestForCompatibilitySince;
import com.hazelcast.version.Version;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static java.lang.String.format;

/**
 * Ignores tests annotated with {@code @TestForCompatibilitySince("X.Y")}
 * as long as the current codebase version is less than {@code X.Y}.
 */
public class IgnoreCompatibilityTestsWithSinceRule implements TestRule {

    private static final ILogger LOGGER = Logger.getLogger(IgnoreCompatibilityTestsWithSinceRule.class);

    @Override
    public Statement apply(Statement base, final Description description) {
        TestForCompatibilitySince testSince = description.getAnnotation(TestForCompatibilitySince.class);
        if (testSince == null) {
            testSince = description.getTestClass().getAnnotation(TestForCompatibilitySince.class);
        }
        if (testSince != null) {
            final Version currentCodebaseVersion = Version.of(BuildInfoProvider.getBuildInfo().getVersion());
            final Version testSinceVersion = Version.of(testSince.value());
            if (currentCodebaseVersion.isLessThan(testSinceVersion)) {
                return new Statement() {
                    @Override
                    public void evaluate() {
                        LOGGER.finest(format(
                                "Ignoring `%s` because it is meant for execution since %s while current version is %s",
                                description.getClassName(), testSinceVersion, currentCodebaseVersion));
                    }
                };
            }
        }
        return base;
    }
}
