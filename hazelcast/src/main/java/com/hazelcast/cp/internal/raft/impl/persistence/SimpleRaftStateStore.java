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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Simple implementation of RaftStateStore.
 * Only for testing...
 */
public class SimpleRaftStateStore implements RaftStateStore {

    static final int BUFFER_CAP = 1 << 10;

    private final File baseDir;
    private final File termTmpPath;
    private final File termPath;
    private FileOutputStream logChannel;
    private ObjectDataOutputStream logDataOut;

    public SimpleRaftStateStore(File dir) {
        baseDir = dir;
        baseDir.mkdirs();
        termTmpPath = new File(dir, "term.tmp");
        termPath = new File(dir, "term");
    }

    @Override
    public void open() throws IOException {
        File activeLogPath = new File(baseDir, "logs");
        logChannel = new FileOutputStream(activeLogPath, true);
        logDataOut = newDataOutputStream(logChannel);
    }

    @Override
    public void persistInitialMembers(RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers) throws IOException {
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
    public void persistTerm(int term, RaftEndpoint electedEndpoint) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(termTmpPath);
        ObjectDataOutputStream out = newDataOutputStream(fileOutputStream);
        try {
            out.writeInt(term);
            out.writeObject(electedEndpoint);
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }
        IOUtil.rename(termTmpPath, termPath);
    }

    @Override
    public void persistEntry(LogEntry entry) throws IOException {
        entry.writeData(logDataOut);
    }

    @Override
    public void persistSnapshot(SnapshotEntry entry) throws IOException {
        File snapshotTmpPath = new File(baseDir, "snapshot.tmp");

        FileOutputStream fileOutputStream = new FileOutputStream(snapshotTmpPath);
        ObjectDataOutputStream out = newDataOutputStream(fileOutputStream);

        try {
            out.writeObject(entry);
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }
        File snapshotPath = new File(baseDir, "snapshot");
        IOUtil.rename(snapshotTmpPath, snapshotPath);
    }

    @Override
    public void deleteEntriesFrom(long startIndexInclusive) throws IOException {
        close();

        File activeLogPath = new File(baseDir, "logs");
        File activeLogTmpPath = new File(baseDir, "logs.tmp");

        LogEntry entry = new LogEntry();

        ObjectDataInputStream in =
                new ObjectDataInputStream(new BufferedInputStream(
                        new FileInputStream(activeLogPath), BUFFER_CAP), getSerializationService());
        FileOutputStream fileOutputStream = new FileOutputStream(activeLogTmpPath);
        ObjectDataOutputStream out = newDataOutputStream(fileOutputStream);

        try {
            for (;;) {
                entry.readData(in);
                if (entry.index() >= startIndexInclusive) {
                    break;
                }
                entry.writeData(out);
            }
            out.flush();
            fileOutputStream.getFD().sync();
        } finally {
            IOUtil.closeResource(fileOutputStream);
            IOUtil.closeResource(out);
        }

        IOUtil.rename(activeLogTmpPath, activeLogPath);

        logChannel = new FileOutputStream(activeLogPath, true);
        logDataOut = newDataOutputStream(logChannel);
    }

    @Override
    public void flushLogs() throws IOException {
        logDataOut.flush();
        logChannel.getFD().sync();
    }

    @Override
    public void close() throws IOException {
        flushLogs();
        logDataOut.close();
        logChannel.close();
    }

    private ObjectDataOutputStream newDataOutputStream(FileOutputStream fileOutputStream) {
        // TODO: get serialization service from Hazelcast node
        return new ObjectDataOutputStream(new BufferedOutputStream(fileOutputStream), getSerializationService());
    }

    private InternalSerializationService getSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

}

