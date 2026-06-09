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

import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.types.VectorFloat;

class HazelcastVectorUtil {
    private HazelcastVectorUtil() {
    }

    public static float cosine(VectorFloat<?> a, VectorFloat<?> b) {
        if (a.length() != b.length()) {
            throw new IllegalArgumentException("vector dimensions differ: " + a.length() + "!=" + b.length());
        }
        float r = cosine0(a, b);
        assert Float.isFinite(r) : String.format("cosine(%s, %s) = %s", a, b, r);
        return r;
    }

    /**
     * Manually unrolled implementation of cosine metric for non-Panama configuration.
     */
    @SuppressWarnings({"checkstyle:magicnumber", "checkstyle:methodlength"})
    private static float cosine0(VectorFloat<?> av, VectorFloat<?> bv) {
        float[] a = ((ArrayVectorFloat) av).get();
        float[] b = ((ArrayVectorFloat) bv).get();

        float norm1 = 0.0f;
        float norm2 = 0.0f;
        float sum = 0f;

        int i;
        for (i = 0; i < a.length % 8; i++) {
            sum += b[i] * a[i];
            norm1 += a[i] * a[i];
            norm2 += b[i] * b[i];
        }
        if (a.length < 8) {
            return (float) (sum / Math.sqrt(norm1 * norm2));
        }

        for (; i + 7 < a.length; i += 8) {
            sum +=
                    b[i + 0] * a[i + 0]
                            + b[i + 1] * a[i + 1]
                            + b[i + 2] * a[i + 2]
                            + b[i + 3] * a[i + 3]
                            + b[i + 4] * a[i + 4]
                            + b[i + 5] * a[i + 5]
                            + b[i + 6] * a[i + 6]
                            + b[i + 7] * a[i + 7];
            norm1 +=
                    a[i + 0] * a[i + 0]
                            + a[i + 1] * a[i + 1]
                            + a[i + 2] * a[i + 2]
                            + a[i + 3] * a[i + 3]
                            + a[i + 4] * a[i + 4]
                            + a[i + 5] * a[i + 5]
                            + a[i + 6] * a[i + 6]
                            + a[i + 7] * a[i + 7];

            norm2 +=
                    b[i + 0] * b[i + 0]
                            + b[i + 1] * b[i + 1]
                            + b[i + 2] * b[i + 2]
                            + b[i + 3] * b[i + 3]
                            + b[i + 4] * b[i + 4]
                            + b[i + 5] * b[i + 5]
                            + b[i + 6] * b[i + 6]
                            + b[i + 7] * b[i + 7];
        }

        return (float) (sum / Math.sqrt(norm1 * norm2));
    }
}
