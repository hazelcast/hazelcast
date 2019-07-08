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
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Simple implementation of RaftStateStore.
 * Only for testing...
 */
public class SimpleRaftStateStore implements RaftStateStore {

    private final File baseDir;
    private final File termTmpPath;
    private final File termPath;
    private final RaftLogStore logStore;

    public SimpleRaftStateStore(File dir) throws IOException {
        baseDir = dir;
        baseDir.mkdirs();
        termTmpPath = new File(dir, "term.tmp");
        termPath = new File(dir, "term");
        logStore = new SimpleRaftLogStore(dir);
    }

    @Override
    public void writeTermAndVote(int currentTerm, RaftEndpoint votedFor, int voteTerm) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(termTmpPath);
        ObjectDataOutputStream out = newDataOutputStream(fileOutputStream);
        try {
            out.writeInt(currentTerm);
            out.writeInt(voteTerm);
            out.writeObject(votedFor);
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }
        IOUtil.rename(termTmpPath, termPath);
    }

    @Override
    public void writeInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) throws IOException {
        File membersTmpPath = new File(baseDir, "members.tmp");
        FileOutputStream fileOutputStream = new FileOutputStream(membersTmpPath);
        ObjectDataOutputStream out = newDataOutputStream(fileOutputStream);
        try {
            out.writeObject(localMember);
            writeCollection(initialMembers, out);
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }
        File membersPath = new File(baseDir, "members");
        IOUtil.rename(membersTmpPath, membersPath);
    }

    @Override
    public RaftLogStore getRaftLogStore() {
        return logStore;
    }

    private ObjectDataOutputStream newDataOutputStream(FileOutputStream fileOutputStream) {
        return new ObjectDataOutputStream(new BufferedOutputStream(fileOutputStream),
                // TODO: get serialization service from Hazelcast node
                new DefaultSerializationServiceBuilder().build());
    }

    @Override
    public void close() throws IOException {
        logStore.close();
    }

}

