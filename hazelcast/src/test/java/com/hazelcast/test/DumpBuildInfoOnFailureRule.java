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

import com.hazelcast.instance.BuildInfoProvider;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Prints the {@link com.hazelcast.instance.BuildInfo} after a test failure.
 * <p>
 * <b>Note:</b> Is automatically added to tests which extend {@link HazelcastTestSupport}.
 */
public class DumpBuildInfoOnFailureRule implements TestRule {

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    printBuildInfo(description);
                    throw t;
                }
            }
        };
    }

    private void printBuildInfo(Description description) {
        System.out.println("BuildInfo right after " + description.getDisplayName() + ": " + BuildInfoProvider.getBuildInfo());
    }
}
