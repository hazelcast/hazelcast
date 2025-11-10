/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.vector.impl.SingleIndexVectorValues;

import java.util.Map;

/**
 * A {@code VectorValues} instance contains the vectors that describe a {@link VectorDocument}.
 * {@code VectorValues} may come in different forms:
 * <ul>
 *     <li>When a single vector index is defined in a {@code VectorCollection}, then a plain
 *     {@code float[]} can be used, as defined in {@link SingleVectorValues}</li>
 *     <li>When more than one vector index is defined, then the vector representation of a
 *     {@link VectorDocument} requires a mapping from index name to the associated
 *     {@code float[]} vector</li>.
 * </ul>
 *
 * @since 5.5
 */
@Beta
public interface VectorValues {

    /**
     * Represents values of a single vector.
     */
    interface SingleVectorValues extends VectorValues {
        float[] vector();
    }

    /**
     * Provides a mapping from index name to vector, supplying vectors for a {@code VectorCollection}
     * with multiple indexes defined.
     */
    interface MultiIndexVectorValues extends VectorValues {
        Map<String, float[]> indexNameToVector();
    }

    /**
     * Returns a {@code VectorValues} object suitable for supplying a single vector to a {@code VectorCollection}
     * configured with one vector index.
     *
     * @param vector the vector
     * @return a {@code VectorValues} object representing the given vector
     */
    static VectorValues of(float[] vector) {
        return new SingleIndexVectorValues(vector);
    }

    /**
     * Returns a {@code VectorValues} object that represents a single named vector.
     * @param indexName the index name
     * @param vector    the vector
     * @return           a {@code VectorValues} representing the given vector with name.
     */
    static VectorValues of(String indexName, float[] vector) {
        return new com.hazelcast.vector.impl.MultiIndexVectorValues(
                Map.of(indexName, vector));
    }

    /**
     * Returns a {@code VectorValues} object that represents two mappings of index names to associated vectors.
     * @param indexName1 the first mapping's index name
     * @param vector1    the first mapping's vector
     * @param indexName2 the second mapping's index name
     * @param vector2    the second mapping's vector
     * @return           a {@code VectorValues} object with two index name to vector mappings.
     */
    static VectorValues of(String indexName1, float[] vector1,
                           String indexName2, float[] vector2) {
        return new com.hazelcast.vector.impl.MultiIndexVectorValues(
                Map.of(indexName1, vector1, indexName2, vector2));
    }

    /**
     * Returns a {@code VectorValues} object that represents the given mappings of index names to associated vectors.
     * @param indexNameToVector the index name to vector mappings
     * @return                  a {@code VectorValues} object populated with the given mappings.
     */
    static VectorValues of(Map<String, float[]> indexNameToVector) {
        return new com.hazelcast.vector.impl.MultiIndexVectorValues(indexNameToVector);
    }
}
