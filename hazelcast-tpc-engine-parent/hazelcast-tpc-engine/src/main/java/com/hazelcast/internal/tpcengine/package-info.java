/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
 * <p>Contains the Thread Per Core engine<br>
 * <p>
 * This whole package is internal and no compatibility will be provided.
 * <p>
 * Network/Kernel tuning references:
 * <ol>
 *     <li>https://talawah.io/blog/linux-kernel-vs-dpdk-http-performance-showdown/</li>
 *     <li>https://talawah.io/blog/extreme-http-performance-tuning-one-point-two-million</li>
 * </ol>>
 *
 * <h1>Thoughts on TPC and various different flavors of design</h1>
 * With a thread per core design, you want to have just 1 thread per core. One
 * way of doing that is to have every thread by self contained; so it can do
 * networking, storage, processing etc and ideally you shoot a request directly
 * into that core by opening a socket to that core. ScyllaDB using their enhanced
 * drivers does this.
 * <p/>
 * But in some cases this isn't possible, e.g. when you just 1 port exposed.
 * This can be solved in multiple ways: e.g. have 1 dedicated reactor taking
 * care of that traffic and potentially send the request to the right reactor. The
 * problems is that eventually the reactor where the traffic is received, will
 * become the bottleneck.
 * <p/>
 * An alternative approach is to let each reactor open the same server port
 * (SO_PORTREUSE) and the connections then will automatically be distributed
 * over the reactors and then each reactor needs to send the request to the
 * right reactor.
 * <p/>
 * If there is no dedicated port per reactor, then the work needs to be send to the
 * right reactor. Currently this can be done using the TaskQueue; but in the future
 * we'll probably want to have a Seastar-like approach where there is a SPSC-queue
 * from each core to each other core. This reduces contention and hence improves
 * scalability.
 * <p/>
 * In theory it is also possible to let each reactor process the work it received
 * and rely on some locking schema to ensure that only 1 thread is active within
 * a certain domain. THe problem is that this approach isn't very scalable due
 * to contention (Amdahls law), and coherence delay (universal scalability law)
 * that can even cause retrograde scalability. Another problem is that you have
 * no control on accessing foreign memory (so memory on a different NUMA node).
 * <p/>
 * In theory also a SEDA like system could be written with TPC; so a system where
 * every core has a different task. It is just a matter of sharing the appropriate
 * state between the reactors. This means that TPC and SEDA are not mutually exclusive.
 * The primary difference is that with a SEDA design you typically have more threads
 * than cores. And example of a SEDA + TPC like design would be Tigerbeetle once they
 * start to offload certain tasks to a dedicated reactor.
 * <p/>
 * A long story short; there are many ways to design a TPC system. The tpc-engine is a
 * flexible design (it doesn't impose restrictions) and therefor can be used in many
 * different ways.
 * <p/>
 * Good read how to design a fast database on top of modenr NVMe based SSDs
 * https://www.vldb.org/pvldb/vol16/p2090-haas.pdf
 */
package com.hazelcast.internal.tpcengine;
