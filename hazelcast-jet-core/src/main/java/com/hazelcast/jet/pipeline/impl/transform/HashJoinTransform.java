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

package com.hazelcast.jet.pipeline.impl.transform;

import com.hazelcast.jet.pipeline.MultiTransform;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.datamodel.Tag;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Represents a hash-join transform, also called a replicated join. It is
 * an asymmetrical join where all positive-numbered streams are joined
 * to stream 0 (this matches the <em>star schema</em> data model). For each
 * positive-numbered stream N there is a pair of key extractor functions:
 * one for the stream-0 item and one from the stream-N item. In SQL terms
 * the operation is equivalent to
 * <pre>
 * stream0 JOIN stream1 ON stream0.key1 == stream1.key
 *         JOIN stream2 ON stream0.key2 == stream2.key
 *         etc.
 * </pre>
 * where all of {@code stream0.key1}, {@code stream0.key2}, {@code
 * stream1.key} and {@code stream2.key} are extracted by separate
 * functions. The pair of functions is captured in an instance of
 * {@link JoinClause}.
 * <p>
 * The data from positive-numbered streams is fully buffered before
 * consuming stream 0, which is processed eagerly without buffering.
 * In detail,
 * <ol><li>
 *     The positive-numbered stream sources broadcast their data to all
 *     hash-join processor instances, which use it to build local hashtables.
 *     A hashtable maps a lookup key to a list of all the items with that key.
 * </li><li>
 *     Consumption of stream 0 begins. For each item a separate lookup key is
 *     extracted for each positive-numbered stream and the result items looked
 *     up from the hashtables. The results are emitted in a tuple.
 * </li></ol>
 * The structure of the output tuple has two variants. In the basic
 * variant, each tuple component maps to the same-numbered inbound stream.
 * Component 0 is a single stream-0 item and the rest are {@code
 * Iterable}s containing the looked-up data. This variant has full static
 * type safety, but only supports two-way and three-way joins.
 * <p>
 * In the other variant the output item type is {@code
 * Tuple2<E0, BagsByTag>} where component 0 is the stream-0 item and
 * component 1 is a container of all the stream-N iterables (which we call
 * "bags"). An individual bag is retrieved using a {@link Tag} instance as
 * key.
 */
public class HashJoinTransform<E0> implements MultiTransform {
    private final List<JoinClause<?, E0, ?, ?>> clauses;
    private final List<Tag> tags;

    public HashJoinTransform(@Nonnull List<JoinClause<?, E0, ?, ?>> clauses, @Nonnull List<Tag> tags) {
        this.clauses = clauses;
        this.tags = tags;
    }

    public List<JoinClause<?, E0, ?, ?>> clauses() {
        return clauses;
    }

    public List<Tag> tags() {
        return tags;
    }

    @Override
    public String toString() {
        return tags.size() + "-way HashJoin";
    }
}
