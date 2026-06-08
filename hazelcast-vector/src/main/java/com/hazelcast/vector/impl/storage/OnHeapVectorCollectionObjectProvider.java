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

import com.hazelcast.vector.VectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;

public class OnHeapVectorCollectionObjectProvider implements VectorCollectionObjectProvider {

    @Override
    public VectorFloatConverter createVectorFloatConverter() {
        return new VectorFloatConverter() {

            @Override
            public VectorFloat<?> toVectorFloat(float[] vector) {
                return ArrayVectorProvider.getInstance().createFloatVector(vector);
            }

            @Override
            public float[] toFloatArray(VectorFloat<?> vectorFloat) {
                return (float[]) vectorFloat.get();
            }

            @Override
            public VectorValues createSingleVectorValue(VectorFloat<?> vectorFloat) {
                float[] floatArray = (float[]) vectorFloat.get();
                return VectorValues.of(floatArray);
            }
        };
    }


    public static VectorCollectionObjectProvider getInstance() {
        return OnHeapVectorCollectionObjectProvider.Holder.INSTANCE;
    }

    private static VectorCollectionObjectProvider createOnHeapTypeSupport() {
        return new OnHeapVectorCollectionObjectProvider();
    }

    private static final class Holder {
        static final VectorCollectionObjectProvider INSTANCE = createOnHeapTypeSupport();

        private Holder() {
        }
    }
}
