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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.Processor;

public final class SnapshotFlags {

    /**
     * If set, the job should terminate after the snapshot is processed.
     */
    private static final int TERMINAL = 1;

    /**
     * If set, the snapshot is an export (with or without cancellation). If not
     * set, it's an automatic snapshot for fault tolerance.
     */
    private static final int EXPORT = 2; // 0b10

    private SnapshotFlags() { }

    public static boolean isTerminal(int flags) {
        return (flags & TERMINAL) != 0;
    }

    public static boolean isExport(int flags) {
        return (flags & EXPORT) != 0;
    }

    /**
     * If set, the {@link Processor#snapshotCommitPrepare()} and {@link
     * Processor#snapshotCommitFinish(boolean)} methods won't be called, only
     * {@link Processor#saveToSnapshot()}.
     * <p>
     * It's initiated with {@link Job#exportSnapshot}, but not with {@link
     * Job#cancelAndExportSnapshot}.
     */
    public static boolean isExportOnly(int flags) {
        return isExport(flags) && !isTerminal(flags);
    }

    public static String toString(int flags) {
        return "terminal=" + (isTerminal(flags) ? "yes" : "no")
                + ",export=" + (isExport(flags) ? "yes" : "no");
    }

    public static int create(boolean isTerminal, boolean isExport) {
        return (isTerminal ? TERMINAL : 0) | (isExport ? EXPORT : 0);
    }
}
