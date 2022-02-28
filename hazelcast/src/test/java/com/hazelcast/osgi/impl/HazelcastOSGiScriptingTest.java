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

package com.hazelcast.osgi.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.osgi.TestBundle;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public abstract class HazelcastOSGiScriptingTest extends HazelcastTestSupport {

    protected TestBundle bundle;

    @Rule
    public TestRule scriptingAvailableRule = new TestRule() {

        @Override
        public Statement apply(Statement base, Description description) {
            if (Activator.isJavaxScriptingAvailable()) {
                return base;
            } else {
                return new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        System.out.println("Ignoring test since scripting is not available!");
                    }
                };
            }
        }

    };

    @Before
    public void setup() throws Exception {
        bundle = new TestBundle();
        bundle.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            bundle.stop();
            bundle = null;
        } finally {
            Hazelcast.shutdownAll();
        }
    }
}
