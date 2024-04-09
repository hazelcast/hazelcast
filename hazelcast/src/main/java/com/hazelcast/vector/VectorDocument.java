/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.vector.impl.VectorDocumentImpl;

/**
 * A {@code VectorDocument} includes a user-supplied {@code V value} and the associated {@link VectorValues}
 * that represent the document.
 * @param <V>
 *
 * @since 5.5
 */
@Beta
public interface VectorDocument<V> {

    V getValue();

    VectorValues getVectors();

    static <T> VectorDocument<T> of(T value, VectorValues vv) {
        return new VectorDocumentImpl<>(value, vv);
    }
}
