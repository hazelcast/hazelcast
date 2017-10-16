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

package com.hazelcast.jet.config;

/**
 * You can choose between {@link #EXACTLY_ONCE} and {@link #AT_LEAST_ONCE}
 * processing. The trade-off is between correctness and performance. It's
 * configured per-job.
 *<ol>
 * <li><i>Exactly once:</i> Favors correctness. Guarantees that each event is
 * processed exactly once by the processors. Might increase latency and
 * decrease throughput due to aligning of the barriers.
 * <li><i>At least once:</i> Events which came after the barrier in the stream
 * might be processed before the snapshot is taken. This will cause their
 * duplicate processing, if the job is restarted.
 *</ol>
 * The distributed snapshot algorithm works by sending <i>barriers</i> down the
 * stream. When one is received by a processor, it must do a state snapshot.
 * However, the processor can have multiple inputs, so to save correct snapshot
 * it must wait until the barrier is received from all other inputs and not
 * process any more items from inputs from which the barrier was already
 * received. If the stream is skewed (due to partition imbalance, long GC pause
 * on some member or a network hiccup), processing has to be halted until the
 * situation recovers. At-least-once mode allows processing of further items
 * during this alignment period.
 */
public enum ProcessingGuarantee {
    /**
     * See {@link ProcessingGuarantee class javadoc}.
     */
    EXACTLY_ONCE,

    /**
     * See {@link ProcessingGuarantee class javadoc}.
     */
    AT_LEAST_ONCE;
}
