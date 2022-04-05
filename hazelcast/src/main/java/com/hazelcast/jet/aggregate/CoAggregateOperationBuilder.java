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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Offers a step-by-step API to create an aggregate operation that
 * accepts multiple inputs. To obtain it, call {@link
 * AggregateOperations#coAggregateOperationBuilder()}. and refer to that
 * method's Javadoc for further details.
 *
 * @since Jet 3.0
 */
public class CoAggregateOperationBuilder {

    private final Map<Tag, AggregateOperation1> opsByTag = new HashMap<>();

    CoAggregateOperationBuilder() { }

    /**
     * Registers the given aggregate operation with the tag corresponding to an
     * input to the co-aggregating operation being built. If you are preparing
     * an operation to pass to an {@linkplain
     * StageWithKeyAndWindow#aggregateBuilder() aggregate builder}, you must
     * use the tags you obtained from it.
     * <p>
     * Returns the tag you'll use to retrieve the results of aggregating this
     * input.
     *
     * @param <T> type of this operation's input
     * @param <R> the result type of this operation
     * @return the result tag
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T, R> Tag<R> add(
            @Nonnull Tag<T> tag,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> operation
    ) {
        opsByTag.put(tag, operation);
        return (Tag<R>) tag;
    }

    /**
     * Builds and returns the {@link AggregateOperation}. Its result type is
     * {@link ItemsByTag} containing all the tags you got from the
     * {@link #add} method.
     */
    @Nonnull
    public AggregateOperation<Object[], ItemsByTag> build() {
        return build(identity());
    }

    /**
     * Builds and returns the multi-input {@link AggregateOperation}. It will
     * call the supplied {@code exportFinishFn} to transform the {@link ItemsByTag}
     * it creates to the result type it emits as the actual result.
     *
     * @param exportFinishFn function to convert {@link ItemsByTag} to the
     *     target result type. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    public <R> AggregateOperation<Object[], R> build(
            @Nonnull FunctionEx<? super ItemsByTag, ? extends R> exportFinishFn
    ) {
        checkSerializable(exportFinishFn, "exportFinishFn");
        Tag[] tags = opsByTag.keySet().stream().sorted().toArray(Tag[]::new);
        for (int i = 0; i < tags.length; i++) {
            Preconditions.checkTrue(tags[i].index() == i, "Registered tags' indices are "
                    + stream(tags).map(Tag::index).collect(toList())
                    + ", but should be " + range(0, tags.length).boxed().collect(toList()));
        }
        // Variable `sorted` extracted due to type inference failure
        Stream<Entry<Tag, AggregateOperation1>> sorted = opsByTag.entrySet().stream()
                                                                 .sorted(comparing(Entry::getKey));
        List<AggregateOperation1> ops = sorted.map(Entry::getValue).collect(toList());
        BiConsumerEx[] combineFns =
                ops.stream().map(AggregateOperation::combineFn).toArray(BiConsumerEx[]::new);
        BiConsumerEx[] deductFns =
                ops.stream().map(AggregateOperation::deductFn).toArray(BiConsumerEx[]::new);
        FunctionEx[] exportFns =
                ops.stream().map(AggregateOperation::exportFn).toArray(FunctionEx[]::new);
        FunctionEx[] finishFns =
                ops.stream().map(AggregateOperation::finishFn).toArray(FunctionEx[]::new);

        AggregateOperationBuilder.VarArity<Object[], Void> b = AggregateOperation
                .withCreate(() -> ops.stream().map(op -> op.createFn().get()).toArray())
                .varArity();
        opsByTag.forEach((tag, op) -> {
            int index = tag.index();
            b.andAccumulate(tag, (acc, item) -> op.accumulateFn().accept(acc[index], item));
        });
        return b.andCombine(stream(combineFns).anyMatch(Objects::isNull) ? null :
                        (acc1, acc2) -> {
                            for (int i = 0; i < combineFns.length; i++) {
                                combineFns[i].accept(acc1[i], acc2[i]);
                            }
                        })
                .andDeduct(stream(deductFns).anyMatch(Objects::isNull) ? null :
                        (acc1, acc2) -> {
                            for (int i = 0; i < deductFns.length; i++) {
                                deductFns[i].accept(acc1[i], acc2[i]);
                            }
                        })
                .<R>andExport(acc -> {
                    ItemsByTag result = new ItemsByTag();
                    for (int i = 0; i < exportFns.length; i++) {
                        result.put(tags[i], exportFns[i].apply(acc[i]));
                    }
                    return exportFinishFn.apply(result);
                })
                .andFinish(acc -> {
                    ItemsByTag result = new ItemsByTag();
                    for (int i = 0; i < finishFns.length; i++) {
                        result.put(tags[i], finishFns[i].apply(acc[i]));
                    }
                    return exportFinishFn.apply(result);
                });
    }
}
