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

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.cp.internal.raft.impl.persistence.SimpleRaftLogStore.BUFFER_CAP;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;

/**
 * Simple implementation of RaftStateLoader.
 * Only for testing...
 */
public class SimpleRaftStateLoader implements RaftStateLoader {

    private final File baseDir;

    public SimpleRaftStateLoader(File dir) {
        baseDir = dir;
    }

    @Override
    public RestoredRaftState load() throws IOException {
        Tuple2<Integer, RaftEndpoint> termAndVote = readVoteAndTerm();
        Tuple2<RaftEndpoint, Collection<RaftEndpoint>> members = readMembers();
        SnapshotEntry snapshot = readSnapshot();
        LogEntry[] entries = readLogs(snapshot.index());

        return new RestoredRaftState(members.element1, members.element2, termAndVote.element1, termAndVote.element2, snapshot,
                entries);
    }

    private Tuple2<Integer, RaftEndpoint> readVoteAndTerm() throws IOException {
        File path = new File(baseDir, "term");
        if (path.exists()) {
            ObjectDataInputStream in = getDataInputStream(path);
            try {
                int term = in.readInt();
                RaftEndpoint votedFor = in.readObject();
                return Tuple2.of(term, votedFor);
            } finally {
                IOUtil.closeResource(in);
            }
        }
        return Tuple2.of(0, null);
    }

    private Tuple2<RaftEndpoint, Collection<RaftEndpoint>> readMembers() throws IOException {
        File path = new File(baseDir, "members");
        if (path.exists()) {
            ObjectDataInputStream in = getDataInputStream(path);
            try {
                RaftEndpoint localMember = in.readObject();
                Collection<RaftEndpoint> initialMembers = readCollection(in);
                return Tuple2.of(localMember, initialMembers);
            } finally {
                IOUtil.closeResource(in);
            }
        }
        return Tuple2.of(null, null);
    }

    private SnapshotEntry readSnapshot() throws IOException {
        SnapshotEntry snapshot = new SnapshotEntry();
        File path = new File(baseDir, "snapshot");
        if (path.exists()) {
            ObjectDataInputStream in = getDataInputStream(path);
            try {
                snapshot.readData(in);
            } finally {
                IOUtil.closeResource(in);
            }
        }
        return snapshot;
    }

    private LogEntry[] readLogs(long snapshotIndex) throws IOException {
        List<LogEntry> entries = new ArrayList<LogEntry>();
        File path = new File(baseDir, "logs");
        if (path.exists()) {
            ObjectDataInputStream in = getDataInputStream(path);
            try {
                while (in.available() > 0) {
                    LogEntry entry = new LogEntry();
                    try {
                        entry.readData(in);
                    } catch (HazelcastSerializationException e) {
                        if (e.getCause() instanceof EOFException) {
                            // partially written entry
                            break;
                        }
                        throw e;
                    }
                    if (entry.index() > snapshotIndex) {
                        entries.add(entry);
                    }
                }
            } finally {
                IOUtil.closeResource(in);
            }
        }
        return entries.toArray(new LogEntry[0]);
    }

    private ObjectDataInputStream getDataInputStream(File path) throws IOException {
        return new ObjectDataInputStream(new BufferedInputStream(new FileInputStream(path), BUFFER_CAP),
                getSerializationService());
    }

    private static InternalSerializationService getSerializationService() {
        // TODO: get serialization service from Hazelcast node
        return new DefaultSerializationServiceBuilder().build();
    }

}
