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

package com.hazelcast.test;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Properties;

/**
 * JUnit 5 extension counterpart of {@link HazelcastSerialClassRunner}.
 * <p>
 * Runs tests serially and isolates system properties per test using a
 * copy-on-write {@link LocalProperties} instance so that property changes
 * made inside a test do not affect subsequent tests.
 * <p>
 * Should be used via {@link SerialTest} annotation:
 * <pre>{@code
 * @SerialTest
 * class MyTest { ... }
 * }</pre>
 */
public class HazelcastSerialTestExtension extends AbstractHazelcastExtension
        implements BeforeEachCallback, AfterEachCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(HazelcastSerialTestExtension.class);
    private static final String SAVED_PROPS_KEY = "savedSystemProperties";

    @Override
    public void beforeEach(ExtensionContext context) {
        // save the current system properties before the test
        Properties currentSystemProperties = System.getProperties();
        context.getStore(NAMESPACE).put(SAVED_PROPS_KEY, currentSystemProperties);
        // use local (copy-on-write) system properties so serial tests don't affect each other
        System.setProperties(new LocalProperties(currentSystemProperties));

        super.beforeEach(context);
    }

    @Override
    public void afterEach(@NonNull ExtensionContext context) {
        try {
            super.afterEach(context);
        } finally {
            // restore the system properties
            Properties savedProperties = context.getStore(NAMESPACE)
                    .remove(SAVED_PROPS_KEY, Properties.class);
            if (savedProperties != null) {
                System.setProperties(savedProperties);
            }
        }
    }

    /**
     * A {@link Properties} subclass that copies all entries from the given parent
     * on construction, so that modifications are local and do not affect the original.
     */
    private static class LocalProperties extends Properties {

        private LocalProperties(Properties properties) {
            this.putAll(properties);
        }
    }
}
