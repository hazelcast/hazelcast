/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.transform;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;

import javax.annotation.Nonnull;

public class PeekTransform<E> implements UnaryTransform<E, E> {
    @Nonnull private final DistributedPredicate<? super E> shouldLogFn;
    @Nonnull private final DistributedFunction<? super E, String> toStringFn;

    public PeekTransform(
            @Nonnull DistributedPredicate<? super E> shouldLogFn,
            @Nonnull DistributedFunction<? super E, String> toStringFn
    ) {
        this.shouldLogFn = shouldLogFn;
        this.toStringFn = toStringFn;
    }

    @Nonnull
    public DistributedPredicate<? super E> shouldLogFn() {
        return shouldLogFn;
    }

    @Nonnull
    public DistributedFunction<? super E, String> toStringFn() {
        return toStringFn;
    }
}
