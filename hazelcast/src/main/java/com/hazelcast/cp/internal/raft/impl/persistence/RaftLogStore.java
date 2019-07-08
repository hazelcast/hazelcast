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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import java.io.Closeable;
import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface RaftLogStore extends Closeable {

    void appendEntry(LogEntry entry) throws IOException;

    void flush() throws IOException;

    void writeSnapshot(SnapshotEntry entry) throws IOException;

    void truncateEntriesFrom(long indexInclusive) throws IOException;

}
