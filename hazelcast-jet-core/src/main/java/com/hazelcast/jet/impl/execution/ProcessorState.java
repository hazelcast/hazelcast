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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.Processor;

/**
 * Describes what a processor is currently doing.
 */
enum ProcessorState {

    /**
     * Making calls to {@link Processor#tryProcessWatermark} until it returns
     * {@code true}.
     */
    PROCESS_WATERMARK,

    /**
     * Waiting to for the outbox to accept the {@link
     * com.hazelcast.jet.core.Watermark}
     */
    EMIT_WATERMARK,

    /**
     * Making calls to {@link Processor#tryProcess()} and {@link
     * Processor#process(int, com.hazelcast.jet.core.Inbox)} until the inbox
     * is empty.
     */
    PROCESS_INBOX,

    /**
     * Making calls to {@link Processor#completeEdge(int)} until it returns
     * {@code true}.
     */
    COMPLETE_EDGE,

    /**
     * Making calls to {@link Processor#complete()} until it returns {@code
     * true}.
     */
    COMPLETE,

    /**
     * Making calls to {@link Processor#saveToSnapshot()} until it returns
     * {@code true}.
     */
    SAVE_SNAPSHOT,

    /**
     * Waiting for the outbox to accept the {@link SnapshotBarrier}.
     */
    EMIT_BARRIER,

    /**
     * Waiting for the outbox to accept the {@code DONE_ITEM}.
     */
    EMIT_DONE_ITEM,

    /**
     * The processor is done.
     */
    END
}
