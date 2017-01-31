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

package com.hazelcast.aggregation.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.query.impl.Extractable;

import java.util.Map;

/**
 * Abstract class providing convenience for concrete implementations of an {@link Aggregator}
 * It also provides the extract() method that enables extracting values of the given attributePath from the input.
 *
 * @param <I> input type
 * @param <R> result type
 */
public abstract class AbstractAggregator<I, R> extends Aggregator<I, R> {

    private final String attributePath;

    public AbstractAggregator() {
        this(null);
    }

    public AbstractAggregator(String attributePath) {
        this.attributePath = attributePath;
    }

    /**
     * Extract the value of the given attributePath from the given entry.
     */
    @SuppressWarnings("unchecked")
    protected final <T> T extract(I input) {
        if (attributePath == null) {
            if (input instanceof Map.Entry) {
                return (T) ((Map.Entry) input).getValue();
            }
        } else if (input instanceof Extractable) {
            return (T) ((Extractable) input).getAttributeValue(attributePath);
        }
        throw new IllegalArgumentException("Can't extract " + attributePath + " from the given input");
    }
}
