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

package com.hazelcast.projection.impl;

import com.hazelcast.projection.Projection;
import com.hazelcast.query.impl.Extractable;

/**
 * Projection that extracts the values of the given attributes and returns them in an Object[] array.
 *
 * @param <I> type of the input
 */
public class MultiAttributeProjection<I> extends Projection<I, Object[]> {

    private final String[] attributePaths;
    private final int attributeCount;

    public MultiAttributeProjection(String... attributePath) {
        if (attributePath.length == 0) {
            throw new IllegalArgumentException("You need to specify at least one attributePath");
        }
        this.attributePaths = attributePath;
        this.attributeCount = attributePath.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] transform(I input) {
        if (input instanceof Extractable) {
            Extractable extractable = ((Extractable) input);
            Object[] result = new Object[attributeCount];
            for (int i = 0; i < attributeCount; i++) {
                result[i] = extractable.getAttributeValue(attributePaths[i]);
            }
            return result;
        }
        throw new IllegalArgumentException("The given map entry is not extractable");
    }

}
