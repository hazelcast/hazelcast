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

/**
 * The Pipeline API is Jet's high-level API to build and execute
 * distributed computation jobs. It models the computation using an analogy
 * with a system of interconnected water pipes. The data flows from the
 * pipeline's sources to its sinks. Pipes can bifurcate and merge, but
 * there can't be any closed loops (cycles).
 * <p>
 * The basic element is a pipeline <em>stage</em> which can be attached to
 * one or more other stages, both in the upstream and the downstream
 * direction. A stage accepts the data coming from its upstream stages,
 * transforms it, and directs the resulting data to its downstream stages.
 *
 * <h2>Kinds of transformation performed by pipeline stages</h2>
 *
 * <h3>Basic</h3>
 *
 * Basic transformations have a single upstream stage and statelessly transform
 * individual items in it. Examples are {@code map}, {@code filter}, and
 * {@code flatMap}.
 *
 * <h3>Grouping and aggregation</h3>
 *
 * The {@code groupBy} transformation groups items by key and performs an
 * aggregate operation on each group. It outputs the results of the
 * aggregate operation, one for each observed distinct key.
 * <p>
 * The {@code coGroup} transformation groups items by key in several
 * streams at once and performs an aggregate operation on all groups that
 * share the same key, separately for each key. It outputs the results of
 * the aggregate operation, one for each observed distinct key.
 *
 * <h3>Hash-join</h3>
 *
 * Hash-join is a special kind of joining transform, specifically tailored
 * to the use case of data enrichment. It is an asymmetrical join that
 * distinguishes the <em>primary</em> upstream stage from the <em>enriching
 * </em> stages. The source for an enriching stage is most typically a
 * key-value store (such as a Hazelcast {@code IMap}). Its data stream must
 * be finite and each item must have a distinct join key. The primary stage
 * may be infinite and contain duplicate keys.
 * <p>
 * For each of the enriching stages there is a separate pair of functions
 * to extract the joining key on both sides. For example, a {@code Trade}
 * can be joined with both a {@code Broker} on {@code trade.getBrokerId()
 * == broker.getId()} and a {@code Product} on {@code trade.getProductId()
 * == product.getId()}, and all this can happen in a single hash-join
 * transform.
 * <p>
 * Implementationally, the hash-join transform is optimized for throughput
 * so that each computing member holds on to all the enriching data, stored
 * in hashtables (hence the name). The enriching streams are consumed in
 * full before ingesting any data from the primary stream.
 */
package com.hazelcast.jet.pipeline;
