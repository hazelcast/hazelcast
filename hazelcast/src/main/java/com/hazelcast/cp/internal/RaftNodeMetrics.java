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

package com.hazelcast.cp.internal;

import com.hazelcast.internal.metrics.Probe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Container object for single RaftNode metrics.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class RaftNodeMetrics {

    @Probe
    public volatile int term;

    @Probe
    public volatile long commitIndex;

    @Probe
    public volatile long lastApplied;

    @Probe
    public volatile long lastLogTerm;

    @Probe
    public volatile long snapshotIndex;

    @Probe
    public volatile long lastLogIndex;

    @Probe
    public volatile long availableLogCapacity;

    void update(int term, long commitIndex, long lastApplied, long lastLogTerm, long snapshotIndex,
            long lastLogIndex, long availableLogCapacity) {
        this.term = term;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.lastLogTerm = lastLogTerm;
        this.snapshotIndex = snapshotIndex;
        this.lastLogIndex = lastLogIndex;
        this.availableLogCapacity = availableLogCapacity;
    }
}
