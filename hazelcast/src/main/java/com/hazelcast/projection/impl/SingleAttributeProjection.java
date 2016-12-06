/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Projection that extracts the values of the given attribute and returns it.
 *
 * @param <I> type of the input
 */
public class SingleAttributeProjection<I, O> extends Projection<I, O> {

    private final String attributePath;

    public SingleAttributeProjection(String attributePath) {
        this.attributePath = attributePath;
    }

    @Override
    @SuppressWarnings("unchecked")
    public O transform(I input) {
        if (input instanceof Extractable) {
            return (O) ((Extractable) input).getAttributeValue(attributePath);
        }
        throw new IllegalArgumentException("The given map entry is not extractable");
    }
}
