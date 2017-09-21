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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Processor;

enum ProcessorState {

    /**
     * Doing calls to {@link Processor#tryProcess()} and {@link
     * Processor#process(int, com.hazelcast.jet.Inbox)} until the inbox is empty.
     */
    PROCESS_INBOX,

    /**
     * Doing calls to {@link Processor#completeEdge(int)} until it returns
     * true.
     */
    COMPLETE_EDGE,

    /**
     * Doing calls to {@link Processor#complete()} until it returns true
     */
    COMPLETE,

    /**
     * Doing calls to {@link Processor#saveToSnapshot()} until
     * it returns true.
     */
    SAVE_SNAPSHOT,

    /**
     * Waiting to accept the {@link SnapshotBarrier} by outbox.
     */
    EMIT_BARRIER,

    /**
     * Waiting until outbox accepts DONE_ITEM.
     */
    EMIT_DONE_ITEM,

    /**
     * Terminal state
     */
    END
}
