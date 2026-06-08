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

package com.hazelcast.vector.impl.storage.graph;

import io.github.jbellis.jvector.vector.VectorUtil;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static com.hazelcast.vector.impl.VectorTestUtils.ensureNonZero;
import static com.hazelcast.vector.impl.VectorTestUtils.randomVectorFloat;
import static org.assertj.core.api.Assertions.assertThat;

class HazelcastBuiltinVectorSimilarityFunctionTest {

    // Due to different order of float operations different rounding errors can be made
    // resulting in small differences in actual values.
    //
    // Does not use percentage due to catastrophic loss of precision in cosine similarity computation
    // via `((1 + cosine) / 2)` formula when cosine is close to -1. In this case vectors are "anti-similar"
    // - the least similarity possible, so the actual precise value of similarity does not matter
    // a lot as long as it is close to 0.
    private static final Offset<Float> TOLERANCE = Offset.offset(0.0001f);

    @Test
    void originalAndUnrolledCosineAreEquivalent() {
        assertEquivalent(HazelcastBuiltinVectorSimilarityFunction.COSINE_UNROLLED_NOT_VECTORIZED, HazelcastBuiltinVectorSimilarityFunction.COSINE, false);
    }

    @Test
    void unrolledCosineEquivalentToDotOnNormalizedVectors() {
        assertEquivalent(HazelcastBuiltinVectorSimilarityFunction.COSINE_UNROLLED_NOT_VECTORIZED, HazelcastBuiltinVectorSimilarityFunction.DOT_PRODUCT, true);
    }

    private void assertEquivalent(HazelcastVectorSimilarityFunction testedFunction, HazelcastVectorSimilarityFunction referenceFunction, boolean normalize) {
        for (int dimension = 1; dimension < 2048; dimension++) {
            var v1 = ensureNonZero(randomVectorFloat(dimension));
            var v2 = ensureNonZero(randomVectorFloat(dimension));
            if (normalize) {
                VectorUtil.l2normalize(v1);
                VectorUtil.l2normalize(v2);
            }

            float referenceScore = referenceFunction.compare(v1, v2);
            assertThat(testedFunction.compare(v1, v2))
                    .as("Should agree for %s and %s", v1, v2)
                    .isCloseTo(referenceScore, TOLERANCE);
            assertThat(testedFunction.compare(v2, v1))
                    .as("Should agree for %s and %s", v2, v1)
                    .isCloseTo(referenceScore, TOLERANCE);
            assertThat(testedFunction.compare(v1, v1))
                    .as("Should be one for the same vector %s", v1)
                    .isCloseTo(1, TOLERANCE);
        }
    }
}
