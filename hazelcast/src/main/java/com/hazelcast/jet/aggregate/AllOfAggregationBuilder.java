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

package com.hazelcast.jet.aggregate;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static com.hazelcast.jet.datamodel.Tag.tag;

/**
 * Offers a step-by-step API to create a composite of multiple aggregate
 * operations. To obtain it, call {@link AggregateOperations#allOfBuilder()}.
 *
 * @param <T> the type of the input items
 *
 * @since Jet 3.0
 */
public final class AllOfAggregationBuilder<T> {

    private final List<Tag> tags = new ArrayList<>();
    private final List<AggregateOperation1> operations = new ArrayList<>();

    AllOfAggregationBuilder() { }

    /**
     * Adds the supplied aggregate operation to the composite. Use the returned
     * {@link Tag} as a key in the {@link ItemsByTag} you get in the result of
     * the composite aggregation.
     *
     * @param <R> the result type of this operation
     */
    @Nonnull
    public <R> Tag<R> add(@Nonnull AggregateOperation1<? super T, ?, R> operation) {
        operations.add(operation);
        Tag<R> tag = tag(tags.size());
        tags.add(tag);
        return tag;
    }

    /**
     * Builds and returns the composite {@link AggregateOperation1}. Its result
     * type is {@link ItemsByTag} containing all the tags you got from the
     * {@link #add} method.
     */
    @Nonnull
    public AggregateOperation1<T, Object[], ItemsByTag> build() {
        return build(identity());
    }

    /**
     * Builds and returns the composite {@link AggregateOperation1}. It will
     * call the supplied {@code exportFinishFn} to transform the {@link ItemsByTag}
     * it creates to the result type it emits as the actual result.
     *
     * @param exportFinishFn function that converts the {@link ItemsByTag} to
     *     the target result type. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public <R> AggregateOperation1<T, Object[], R> build(@Nonnull FunctionEx<ItemsByTag, R> exportFinishFn) {
        checkSerializable(exportFinishFn, "exportFinishFn");

        // Avoid capturing this builder in the lambdas:
        List<Tag> tags = this.tags;
        List<AggregateOperation1> operations = this.operations;

        return (AggregateOperation1<T, Object[], R>) AggregateOperation
                .withCreate(() -> {
                    Object[] acc = new Object[tags.size()];
                    Arrays.setAll(acc, i -> operations.get(i).createFn().get());
                    return acc;
                })
                .andAccumulate((acc, item) -> {
                    for (int i = 0; i < acc.length; i++) {
                        operations.get(i).accumulateFn().accept(acc[i], item);
                    }
                })
                .andCombine(operations.stream().anyMatch(o -> o.combineFn() == null) ? null :
                        (acc1, acc2) -> {
                            for (int i = 0; i < acc1.length; i++) {
                                operations.get(i).combineFn().accept(acc1[i], acc2[i]);
                            }
                        }
                )
                .andDeduct(operations.stream().anyMatch(o -> o.deductFn() == null) ? null :
                        (acc1, acc2) -> {
                            for (int i = 0; i < acc1.length; i++) {
                                operations.get(i).deductFn().accept(acc1[i], acc2[i]);
                            }
                        }
                )
                .andExport(acc -> {
                    ItemsByTag result = new ItemsByTag();
                    for (int i = 0; i < tags.size(); i++) {
                        Object exportedVal = operations.get(i).exportFn().apply(acc[i]);
                        result.put(tags.get(i), exportedVal);
                    }
                    return exportFinishFn.apply(result);
                })
                .andFinish(acc -> {
                    ItemsByTag result = new ItemsByTag();
                    for (int i = 0; i < tags.size(); i++) {
                        Object finishedVal = operations.get(i).finishFn().apply(acc[i]);
                        result.put(tags.get(i), finishedVal);
                    }
                    return exportFinishFn.apply(result);
                });
    }
}
