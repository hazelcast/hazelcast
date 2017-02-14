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

package com.hazelcast.projection;

import com.hazelcast.projection.impl.MultiAttributeProjection;
import com.hazelcast.projection.impl.SingleAttributeProjection;

/**
 * A utility class to create basic {@link com.hazelcast.projection.Projection} instances. <br/>
 *
 * @since 3.8
 */
public final class Projections {

    private Projections() {
    }

    /**
     * Returns a projection that extracts the value of the given attributePath
     *
     * @param attributePath single attribute path, path must not be null or empty
     * @param <O>           Output type
     * @return a projection that extracts the value of the given attributePath
     */
    public static <I, O> Projection<I, O> singleAttribute(String attributePath) {
        return new SingleAttributeProjection<I, O>(attributePath);
    }

    /**
     * Returns a projection that extracts the value of the given attributePaths.
     * The attribute values will be returned as an Object[] array from each projection call.
     *
     * @param attributePath 1 to N attribute Paths, paths must not be null or empty
     * @return a projection that extracts the value of the given attributePaths.
     */
    public static <I> Projection<I, Object[]> multiAttribute(String... attributePath) {
        return new MultiAttributeProjection<I>(attributePath);
    }

}
