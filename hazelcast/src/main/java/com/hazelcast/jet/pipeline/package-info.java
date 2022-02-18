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

/**
 * The Pipeline API is Jet's high-level API to build and execute
 * distributed computation jobs. It models the computation using an analogy
 * with a system of interconnected water pipes. The data flows from the
 * pipeline's sources to its sinks. Pipes can bifurcate and merge, but
 * there can't be any closed loops (cycles).
 * <p>
 * The basic element is a pipeline <em>stage</em> which can be attached to
 * one or more other stages, both in the upstream and the downstream
 * direction. A pipeline accepts the data coming from its upstream stages,
 * transforms it, and directs the resulting data to its downstream stages.
 *
 * <h2>Kinds of transformation performed by pipeline stages</h2>
 *
 * <h3>Basic</h3>
 *
 * Basic transformations have a single upstream pipeline and statelessly
 * transform individual items in it. Examples are {@code map}, {@code
 * filter}, and {@code flatMap}.
 *
 * <h3>Grouping and aggregation</h3>
 *
 * The {@code aggregate*()} transformations perform an aggregate operation
 * on a set of items. You can call {@code stage.groupingKey()} to group the
 * items by a key and then Jet will aggregate each group separately. For
 * stream stages you must specify a {@code stage.window()} which will
 * transform the infinite stream into a series of finite windows. If you
 * specify more than one input stage for the aggregation (using {@code
 * stage.aggregate2()}, {@code stage.aggregate3()} or {@code
 * stage.aggregateBuilder()}, the data from all streams will be combined
 * into the aggregation result. The {@link
 * com.hazelcast.jet.aggregate.AggregateOperation AggregateOperation} you
 * supply must define a separate {@link
 * com.hazelcast.jet.aggregate.AggregateOperation#accumulateFn accumulate}
 * primitive for each contributing stream. Refer to its Javadoc for further
 * details.
 *
 * <h3>Hash-Join</h3>
 *
 * The hash-join is a joining transform designed for the use case of data
 * enrichment with static data. It is an asymmetric join that joins the
 * <em>enriching</em> stage(s) to the <em>primary</em> stage. The
 * <em>enriching</em> stages must be batch stages &mdash; they must
 * represent finite datasets. The primary stage may be either a batch or a
 * stream stage.
 * <p>
 * You must provide a separate pair of functions for each of the enriching
 * stages: one to extract the key from the primary item and one to extract
 * it from the enriching item. For example, you can join a {@code Trade}
 * with a {@code Broker} on {@code trade.getBrokerId() == broker.getId()}
 * and a {@code Product} on {@code trade.getProductId() == product.getId()},
 * and all this can happen in a single hash-join transform.
 * <p>
 * The hash-join transform is optimized for throughput &mdash; each cluster
 * member materializes a local copy of all the enriching data, stored in
 * hashtables (hence the name). It consumes the enriching streams in full
 * before ingesting any data from the primary stream.
 * <p>
 * The output of {@code hashJoin} is just like an SQL left outer join:
 * for each primary item there are N output items,  one for each matching
 * item in the enriching set. If an enriching set doesn't have a matching
 * item, the output will have a {@code null} instead of the enriching item.
 * <p>
 * If you need SQL inner join, then you can use the specialised
 * {@code innerHashJoin} function, in which for each primary item with
 * at least one match, there are N output items, one for each matching
 * item in the enriching set. If an enriching set doesn't have a matching
 * item, there will be no records with the given primary item. In this case
 * the output function's arguments are always non-null.
 *
 * <p>
 * The join also allows duplicate keys on both enriching and primary inputs:
 * the output is a cartesian product of all the matching entries.

 * <p>
 * Example:<pre>
 * +------------------------+-----------------+---------------------------+
 * |     Primary input      | Enriching input |          Output           |
 * +------------------------+-----------------+---------------------------+
 * | Trade{ticker=AA,amt=1} | Ticker{id=AA}   | Tuple2{                   |
 * | Trade{ticker=BB,amt=2} | Ticker{id=BB}   |   Trade{ticker=AA,amt=1}, |
 * | Trade{ticker=AA,amt=3} |                 |   Ticker{id=AA}           |
 * |                        |                 | }                         |
 * |                        |                 | Tuple2{                   |
 * |                        |                 |   Trade{ticker=BB,amt=2}, |
 * |                        |                 |   Ticker{id=BB}           |
 * |                        |                 | }                         |
 * |                        |                 | Tuple2{                   |
 * |                        |                 |   Trade{ticker=AA,amt=3}, |
 * |                        |                 |   Ticker{id=AA}           |
 * |                        |                 | }                         |
 * +------------------------+-----------------+---------------------------+
 * </pre>
 *
 * @since Jet 3.0
 */
package com.hazelcast.jet.pipeline;
