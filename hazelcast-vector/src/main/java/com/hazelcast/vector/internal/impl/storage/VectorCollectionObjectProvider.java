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

package com.hazelcast.vector.internal.impl.storage;

import com.hazelcast.vector.VectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * This class offers various methods to create objects based on the memory type used by the collection.
 *
 * @see OnHeapVectorCollectionObjectProvider for on-heap memory
 */
public interface VectorCollectionObjectProvider {

    /**
     * Creates an instance of {@link VectorFloatConverter}, which is used to convert between
     * different representations of vector data (e.g., float arrays and VectorFloat objects).
     *
     * @return a new instance of {@link VectorFloatConverter}
     */
    VectorFloatConverter createVectorFloatConverter();

    interface VectorFloatConverter {

        /**
         * Converts a float array into a {@link VectorFloat}, depending on the type of memory
         * used.
         *
         * @param vector the float array to be converted
         * @return the corresponding {@link VectorFloat}
         */
        VectorFloat<?> toVectorFloat(float[] vector);

        /**
         * Converts a {@link VectorFloat} back into a float array. This is the reverse operation
         * of {@link #toVectorFloat(float[])}, allowing retrieval of the original float array
         * from a custom vector format.
         *
         * @param vectorFloat the {@link VectorFloat} to be converted
         * @return the corresponding float array
         */
        float[] toFloatArray(VectorFloat<?> vectorFloat);

        /**
         * Converts a {@link VectorFloat} into a {@link VectorValues}, depending on the type
         * of memory used. This method is typically used to return search results from vector
         * storage by converting a custom format into VectorFloat form.
         *
         * @param vectorFloat the {@link VectorFloat} to be converted
         * @return the corresponding {@link VectorValues}
         */
        VectorValues createSingleVectorValue(VectorFloat<?> vectorFloat);
    }
}
