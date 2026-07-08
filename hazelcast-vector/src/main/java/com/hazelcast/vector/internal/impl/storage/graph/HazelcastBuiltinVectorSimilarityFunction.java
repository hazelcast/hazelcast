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

package com.hazelcast.vector.internal.impl.storage.graph;

import com.hazelcast.vector.internal.impl.VectorPlatformUtil;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * Builtin vector similarity functions.
 *
 * @see VectorSimilarityFunction
 */
public enum HazelcastBuiltinVectorSimilarityFunction implements HazelcastVectorSimilarityFunction {

    EUCLIDEAN {
        @Override
        public float compare(VectorFloat<?> v1, VectorFloat<?> v2) {
            return 1 / (1 + VectorUtil.squareL2Distance(v1, v2));
        }
    },

    DOT_PRODUCT {
        @Override
        public float compare(VectorFloat<?> v1, VectorFloat<?> v2) {
            return (1 + VectorUtil.dotProduct(v1, v2)) / 2;
        }
    },

    COSINE {
        @Override
        public float compare(VectorFloat<?> v1, VectorFloat<?> v2) {
            return (1 + VectorUtil.cosine(v1, v2)) / 2;
        }
    },

    COSINE_UNROLLED_NOT_VECTORIZED {
        @Override
        public float compare(VectorFloat<?> v1, VectorFloat<?> v2) {
            return (1 + HazelcastVectorUtil.cosine(v1, v2)) / 2;
        }
    };

    /**
     * Chooses best cosine metric implementation for given configuration
     * avoiding checks during actual execution.
     *
     * @return chosen cosine metric implementation
     */
    public static HazelcastBuiltinVectorSimilarityFunction cosine() {
        if (VectorPlatformUtil.isVectorApiAvailable()) {
            // use vectorized implementation from JVector
            return HazelcastBuiltinVectorSimilarityFunction.COSINE;
        } else {
            return HazelcastBuiltinVectorSimilarityFunction.COSINE_UNROLLED_NOT_VECTORIZED;
        }
    }
}
