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

package com.hazelcast.aggregation.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.internal.json.NonTerminalJsonValue;
import com.hazelcast.query.impl.Extractable;
import com.hazelcast.query.impl.getters.MultiResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract class providing convenience for concrete implementations of an {@link Aggregator}
 * It provides built-in extraction capabilities that may be used in the accumulation phase.
 * <p>
 * Extraction rules:
 * <ul>
 * <li>If the attributePath is not null and the input object is an instance of Extractable the value will be extracted
 * from the attributePath in the input object and accumulated instead of the whole input object.
 * </li>
 * <li>If the attributePath is null and the input object is an instance of Map.Entry the Map.Entry.getValue() will be
 * accumulated instead of the whole input object.
 * </li>
 * </ul>
 *
 * @param <I> input type
 * @param <E> extracted value type
 * @param <R> result type
 */
public abstract class AbstractAggregator<I, E, R> implements Aggregator<I, R> {

    protected String attributePath;

    public AbstractAggregator() {
        this(null);
    }

    public AbstractAggregator(String attributePath) {
        this.attributePath = attributePath;
    }

    @Override
    public final void accumulate(I entry) {

        E extractedValue = extract(entry);
        if (extractedValue instanceof MultiResult) {
            boolean nullEmptyTargetSkipped = false;
            @SuppressWarnings("unchecked")
            MultiResult<E> multiResult = (MultiResult<E>) extractedValue;
            List<E> results = multiResult.getResults();
            for (int i = 0; i < results.size(); i++) {
                E result = results.get(i);
                if (result == null && multiResult.isNullEmptyTarget() && !nullEmptyTargetSkipped) {
                    // if a null or empty target is reached there will be a single null added to the multi-result.
                    // in aggregators we do not care about this null so we have to skip it.
                    // if there are more nulls in the multi-result, they have been added as values.
                    nullEmptyTargetSkipped = true;
                    continue;
                }
                accumulateExtracted(entry, results.get(i));
            }
        } else if (extractedValue != NonTerminalJsonValue.INSTANCE) {
            accumulateExtracted(entry, extractedValue);
        }
    }

    /**
     * Extract the value of the given attributePath from the given entry.
     */
    @SuppressWarnings("unchecked")
    private <T> T extract(I input) {
        if (attributePath == null) {
            if (input instanceof Map.Entry) {
                return (T) ((Map.Entry) input).getValue();
            }
        } else if (input instanceof Extractable) {
            return (T) ((Extractable) input).getAttributeValue(attributePath);
        }
        throw new IllegalArgumentException("Can't extract " + attributePath + " from the given input");
    }

    /**
     * Accumulates a single extracted value.
     * This method may be called multiple times per accumulated entry if the attributePath contains [any] operator.
     *
     * @param entry The entry containing the value.
     * @param value If attributePath is not null the methods accumulates the value extracted from the attributePath.
     *              If attributePath is null and the input object is a Map.Entry the method accumulates Map.Entry.getValue().
     */
    protected abstract void accumulateExtracted(I entry, E value);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractAggregator<?, ?, ?> that = (AbstractAggregator<?, ?, ?>) o;
        return attributePath.equals(that.attributePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributePath);
    }
}
