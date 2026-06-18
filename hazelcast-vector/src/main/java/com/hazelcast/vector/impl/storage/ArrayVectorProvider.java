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

package com.hazelcast.vector.impl.storage;

import io.github.jbellis.jvector.vector.DefaultVectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

/**
 * Provider for on-heap vector type support. Responsible for creating on-heap VectorFloat objects.
 */
public class ArrayVectorProvider {

    private ArrayVectorProvider() {
    }

    public static VectorTypeSupport getInstance() {
        return ArrayVectorProvider.Holder.INSTANCE;
    }

    private static VectorTypeSupport createDefaultTypeSupport() {
        return new DefaultVectorizationProvider().getVectorTypeSupport();
    }

    private static final class Holder {
        static final VectorTypeSupport INSTANCE = createDefaultTypeSupport();

        private Holder() {
        }
    }
}
