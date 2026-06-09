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

package com.hazelcast.vector.impl;

import io.github.jbellis.jvector.vector.DefaultVectorizationProvider;
import io.github.jbellis.jvector.vector.VectorizationProvider;

public final class VectorPlatformUtil {

    private VectorPlatformUtil() {
    }

    /**
     * Checks if optimized vector API is available.
     * <p>
     * This checks if JVector can use optimized metric implementations. If so, they are used.
     * Otherwise, we substitute some of them (cosine metric).
     * This is also an indirect way of checking if Panama Vector API is enabled
     * without duplicating the logic from JVector.
     * If other than default vectorization provider was selected, we infer that
     * Panama Vector API is usable - all other JVector providers require Panama.
     *
     * @return true if optimized vector API is available, false otherwise
     */
    public static boolean isVectorApiAvailable() {
        return !(VectorizationProvider.getInstance() instanceof DefaultVectorizationProvider);
    }

    /**
     * Tests if jdk.incubator.vector module is present within the set of
     * active modules. Sufficient for Java 17, 21 and 25. This is loosely
     * based on jvector's internal test for vector module presence but
     * simplified for the Java versions we care about. Note that this test
     * does not assert the use of the vector module.
     */
    public static boolean isVectorModulePresent() {
        return ModuleLayer.boot().modules().stream()
            .anyMatch(m -> m.getName().equals("jdk.incubator.vector"));
    }
}
