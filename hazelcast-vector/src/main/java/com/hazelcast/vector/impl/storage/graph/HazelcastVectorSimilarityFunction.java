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

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * Interface for similarity functions, more general than {@link VectorSimilarityFunction} enum.
 * @see VectorSimilarityFunction
 */
public interface HazelcastVectorSimilarityFunction {
    /**
     * Calculates a similarity score between the two vectors with a specified function. Higher
     * similarity scores correspond to closer vectors.
     *
     * @param v1 a vector
     * @param v2 another vector, of the same dimension
     * @return the value of the similarity function applied to the two vectors
     *
     * @see VectorSimilarityFunction#compare(VectorFloat, VectorFloat)
     */
    float compare(VectorFloat<?> v1, VectorFloat<?> v2);
}
