/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.ToLongFunctionEx;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static java.lang.Math.max;

abstract class StatefulKeyedTransformBase<T, K, S> extends AbstractTransform {

    private static final int TTL_TO_WM_STRIDE_RATIO = 10;

    final long ttl;
    final FunctionEx<? super T, ? extends K> keyFn;
    final ToLongFunctionEx<? super T> timestampFn;
    final Supplier<? extends S> createFn;

    StatefulKeyedTransformBase(
            @Nonnull String name,
            @Nonnull Transform upstream,
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn
    ) {
        super(name, upstream);
        this.ttl = ttl;
        this.keyFn = keyFn;
        this.timestampFn = timestampFn;
        this.createFn = createFn;
    }

    @Override
    public final long preferredWatermarkStride() {
        return max(1, ttl / TTL_TO_WM_STRIDE_RATIO);
    }
}
