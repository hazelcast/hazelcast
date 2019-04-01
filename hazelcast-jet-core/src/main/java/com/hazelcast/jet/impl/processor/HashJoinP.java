/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.processor.HashJoinCollectP.HashJoinArrayList;
import com.hazelcast.jet.pipeline.BatchStage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Implements the {@linkplain HashJoinTransform hash-join transform}. On
 * all edges except 0 it receives a single item &mdash; the lookup table
 * for that edge (a {@code Map}) and then it processes edge 0 by joining
 * to each item the data from the lookup tables.
 * <p>
 * It extracts a separate key for each of the lookup tables using the
 * functions supplied in the {@code keyFns} argument. Element 0 in that
 * list corresponds to the lookup table received at ordinal 1 and so on.
 * <p>
 * It uses the {@code tags} list to populate the output items. It can be
 * {@code null}, in which case {@code keyFns} must have either one or two
 * elements, corresponding to the two supported special cases in {@link
 * BatchStage} (hash-joining with one or two enriching streams).
 * <p>
 * After looking up all the joined items the processor calls the supplied
 * {@code mapToOutput*Fn} to get the final output item. It uses {@code
 * mapToOutputBiFn} both for the single-arity case ({@code tags == null &&
 * keyFns.size() == 1}) and the variable-arity case ({@code tags != null}).
 * In the latter case the function must expect {@code ItemsByTag} as the
 * second argument. It uses {@code mapToOutputTriFn} for the two-arity
 * case ({@code tags == null && keyFns.size() == 2}).
 */
@SuppressWarnings("unchecked")
public class HashJoinP<E0> extends AbstractProcessor {

    private static final List<Object> LIST_WITH_NULL = singletonList(null);

    private final List<Function<E0, Object>> keyFns;
    private final List<Map<Object, Object>> lookupTables;

    private final FlatMapper<E0, Object> flatMapper;

    private boolean ordinal0consumed;

    public HashJoinP(
            @Nonnull List<Function<E0, Object>> keyFns,
            @Nonnull List<Tag> tags,
            @Nullable BiFunction mapToOutputBiFn,
            @Nullable TriFunction mapToOutputTriFn
    ) {
        this.keyFns = keyFns;
        this.lookupTables = new ArrayList<>(Collections.nCopies(keyFns.size(), null));
        if (!tags.isEmpty()) {
            this.flatMapper = itemsByTagFlatMapper(tags, mapToOutputBiFn);
            return;
        }
        if (keyFns.size() == 1) {
            BiFunction mapToOutput = requireNonNull(mapToOutputBiFn,
                    "tags.isEmpty() && keyFns.size() == 1, but mapToOutputBiFn == null");
            this.flatMapper = flatMapper(item -> traverseStream(lookupJoined(0, item).stream()
                    .map(joinedObject -> mapToOutput.apply(item, joinedObject))
                    .filter(Objects::nonNull)));
            return;
        }
        checkTrue(keyFns.size() == 2, "tags.isEmpty(), but keyFns.size() is neither 1 nor 2");
        TriFunction mapToOutput = requireNonNull(mapToOutputTriFn,
                "tags.isEmpty() && keyFns.size() == 2, but mapToOutputTriFn == null");
        this.flatMapper = flatMapper(object -> {
            List<Object> joined0 = lookupJoined(0, object);
            List<Object> joined1 = lookupJoined(1, object);
            return traverseStream(joined0.stream()
                    .flatMap(firstJoined -> joined1.stream()
                            .map(secondJoined -> mapToOutput.apply(object, firstJoined, secondJoined)))
                    .filter(Objects::nonNull));
        });
    }

    private FlatMapper<E0, Object> itemsByTagFlatMapper(@Nonnull List<Tag> tags, @Nullable BiFunction mapToOutputBiFn) {
        assert mapToOutputBiFn != null : "mapToOutputBiFn required";
        List<Object>[] lookedUpValues = new List[keyFns.size()];
        List<ItemsByTag> values = new ArrayList<>();
        int[] indices = new int[keyFns.size()];
        return flatMapper(primaryItem -> {
            for (int i = 0; i < keyFns.size(); i++) {
                lookedUpValues[i] = lookupJoined(i, primaryItem);
            }
            Arrays.fill(indices, 0);
            values.clear();
            while (indices[0] < lookedUpValues[0].size()) {
                ItemsByTag val = new ItemsByTag();
                for (int j = 0; j < lookedUpValues.length; j++) {
                    val.put(tags.get(j), lookedUpValues[j].get(indices[j]));
                }
                values.add(val);
                for (int j = indices.length - 1; j >= 0; j--) {
                    indices[j]++;
                    if (j == 0 || indices[j] < lookedUpValues[j].size()) {
                        break;
                    }
                    indices[j] = 0;
                }
            }
            return traverseIterable(values)
                    .map(map -> mapToOutputBiFn.apply(primaryItem, map));
        });
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0consumed : "Edge 0 must have a lower priority than all other edges";
        lookupTables.set(ordinal - 1, (Map) item);
        return true;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        E0 e0 = (E0) item;
        ordinal0consumed = true;
        return flatMapper.tryProcess(e0);
    }

    @Nonnull
    private List<Object> lookupJoined(int index, E0 item) {
        Map<Object, Object> lookupTableForOrdinal = lookupTables.get(index);

        Object lookupTableKey = keyFns.get(index).apply(item);
        Object objects = lookupTableForOrdinal.get(lookupTableKey);

        return objects == null ? LIST_WITH_NULL
                : objects instanceof HashJoinArrayList ? (List<Object>) objects
                : singletonList(objects);
    }
}
