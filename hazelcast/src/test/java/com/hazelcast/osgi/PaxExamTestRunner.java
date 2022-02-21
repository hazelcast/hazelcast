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

package com.hazelcast.osgi;

import com.hazelcast.internal.util.JavaVersion;
import org.junit.Assume;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;

/**
 * An outdated PaxRunner used by the OSGi test prevents executing on Java 9+.
 * This JUnit runner assumes Java version is at most 1.8. Otherwise test is ignored.
 *
 * TODO: Remove this assumption once we have a proper fix in the test class.
 */
public class PaxExamTestRunner extends JUnit4TestRunner {

    public PaxExamTestRunner(Class<?> klass) throws Exception {
        super(klass);
    }

    @Override
    protected Statement methodInvoker(FrameworkMethod method, Object test) {
        final Statement statement = super.methodInvoker(method, test);
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Assume.assumeTrue(JavaVersion.isAtMost(JavaVersion.JAVA_8));
                statement.evaluate();
            }
        };
    }
}
