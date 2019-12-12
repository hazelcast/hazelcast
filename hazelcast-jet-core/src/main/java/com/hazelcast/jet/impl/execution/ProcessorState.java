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

import com.hazelcast.jet.core.Inbox;
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
     * Making calls to the zero-argument {@link Processor#tryProcess()} method
     * until it returns true.
     */
    NULLARY_PROCESS,

    /**
     * Making calls to {@link Processor#process(int, Inbox)} until the inbox is
     * empty.
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
     * Making calls to {@link Processor#snapshotCommitPrepare()} until it
     * returns {@code true}.
     */
    SNAPSHOT_PREPARE_COMMIT,

    /**
     * Waiting for the outbox to accept the {@link SnapshotBarrier}.
     */
    EMIT_BARRIER,

    /**
     * Making calls to {@link Processor#snapshotCommitFinish(boolean)} until it
     * returns {@code true}.
     */
    ON_SNAPSHOT_COMPLETED,

    /**
     * Processor completed after a phase 1 of a snapshot and is not doing
     * anything, but it cannot be done until a phase 2 is done - this state is
     * waiting for a phase 2.
     */
    WAITING_FOR_SNAPSHOT_COMPLETED,

    /**
     * Same as {@link #ON_SNAPSHOT_COMPLETED}, but after the processor
     * completed. It follows after {@link #WAITING_FOR_SNAPSHOT_COMPLETED} and
     * is followed by {@link #EMIT_DONE_ITEM}.
     */
    FINAL_ON_SNAPSHOT_COMPLETED,

    /**
     * Waiting for the outbox to accept the {@code DONE_ITEM}.
     */
    EMIT_DONE_ITEM,

    /**
     * The processor is done.
     */
    END
}
