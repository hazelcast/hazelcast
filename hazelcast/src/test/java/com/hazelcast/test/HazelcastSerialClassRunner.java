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

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.Map;
import java.util.Properties;

/**
 * Run the tests in series and log the duration of the running test.
 */
public class HazelcastSerialClassRunner extends AbstractHazelcastClassRunner {

    public HazelcastSerialClassRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    public HazelcastSerialClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
        super(clazz, parameters, name);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        // save the current system properties
        Properties currentSystemProperties = System.getProperties();
        String testName = testName(method);
        setThreadLocalTestMethodName(testName);
        try {
            // use local system properties so se tests don't effect each other
            System.setProperties(new LocalProperties(currentSystemProperties));
            long start = System.currentTimeMillis();
            System.out.println("Started Running Test: " + testName);
            super.runChild(method, notifier);
            float took = (float) (System.currentTimeMillis() - start) / 1000;
            System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
        } finally {
            removeThreadLocalTestMethodName();
            // restore the system properties
            System.setProperties(currentSystemProperties);
        }
    }

    private static class LocalProperties extends Properties {

        private LocalProperties(Properties properties) {
            init(properties);
        }

        private void init(Properties properties) {
            for (Map.Entry entry : properties.entrySet()) {
                put(entry.getKey(), entry.getValue());
            }
        }
    }
}
