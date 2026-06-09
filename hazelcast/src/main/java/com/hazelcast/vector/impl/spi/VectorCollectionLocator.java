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

package com.hazelcast.vector.impl.spi;

import java.util.ServiceLoader;

/**
 * Locator for {@link VectorCollectionProvider} implementations using Java {@link ServiceLoader}.
 *
 * <p>This class is responsible for detecting whether the Vector module is present on the
 * application classpath and exposing the corresponding {@link VectorCollectionProvider}
 * implementation if available.</p>
 *
 * <h2>Module detection</h2>
 * <p>The provider is resolved once at class initialization time using:</p>
 * <pre>{@code
 * ServiceLoader.load(VectorCollectionProvider.class).findFirst()
 * }</pre>
 *
 * <p>If no implementation is found, the Vector module is considered unavailable.</p>
 *
 * @see VectorCollectionProvider
 * @see ServiceLoader
 */
public class VectorCollectionLocator {

    public static final String MISSED_VECTOR_MODULE_MESSAGE = "VectorCollection requires the hazelcast-vector module, "
        + "but hazelcast-vector is not present on the classpath. "
        + "Add the 'hazelcast-vector' JAR dependency to all Hazelcast members "
        + "and all Java clients (if used) that access vector collections. "
        + "You can obtain it from Maven Central. "
        + "See the Hazelcast documentation for configuration and deployment details.";

    private static final VectorCollectionProvider PROVIDER = load();

    private VectorCollectionLocator() {
    }

    private static VectorCollectionProvider load() {
        return ServiceLoader.load(VectorCollectionProvider.class)
            .findFirst()
            .orElse(null);
    }

    public static void assertAvailable() {
        if (!isAvailable()) {
            throw new UnsupportedOperationException(MISSED_VECTOR_MODULE_MESSAGE);
        }
    }

    public static boolean isAvailable() {
        return PROVIDER != null;
    }

    public static VectorCollectionProvider get() {
        assertAvailable();
        return PROVIDER;
    }
}
