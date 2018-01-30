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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.function.DistributedFunction.identity;

/**
 * Use {@link AggregateOperations#allOfBuilder()} to create.
 *
 * @param <T>
 */
public final class AllOfAggregationBuilder<T> {

    private List<Tag> tags = new ArrayList<>();
    private List<AggregateOperation1> operations = new ArrayList<>();

    AllOfAggregationBuilder() { }

    /**
     * Adds one aggregate operation to the composite. Use the returned {@link
     * Tag} to query the final {@link ItemsByTag}.
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
     * Builds the final {@link AggregateOperation1}. The return type will be
     * {@link ItemsByTag}.
     */
    @Nonnull
    public AggregateOperation1<T, Object[], ItemsByTag> build() {
        return build(identity());
    }

    /**
     * Builds the final {@link AggregateOperation1}.
     *
     * @param finishFn function to convert {@link ItemsByTag} to target result type
     */
    @Nonnull
    public <R> AggregateOperation1<T, Object[], R> build(@Nonnull DistributedFunction<ItemsByTag, R> finishFn) {
        return (AggregateOperation1<T, Object[], R>) AggregateOperation
                .withCreate(() -> {
                    Object[] acc = new Object[tags.size()];
                    for (int i = 0; i < acc.length; i++) {
                        acc[i] = operations.get(i).createFn().get();
                    }
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
                .andFinish(acc -> {
                    ItemsByTag result = new ItemsByTag();
                    for (int i = 0; i < tags.size(); i++) {
                        Object finishedVal = operations.get(i).finishFn().apply(acc[i]);
                        result.put(tags.get(i), finishedVal);
                    }
                    return finishFn.apply(result);
                });
    }
}
