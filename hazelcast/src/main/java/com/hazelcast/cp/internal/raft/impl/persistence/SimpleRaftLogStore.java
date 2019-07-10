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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Simple implementation of RaftLogStore.
 * Only for testing...
 */
public class SimpleRaftLogStore implements RaftLogStore {

    static final int BUFFER_CAP = 1 << 10;

    private final File baseDir;

    private FileOutputStream logChannel;
    private ObjectDataOutputStream logDataOut;

    public SimpleRaftLogStore(File dir) {
        baseDir = dir;
        baseDir.mkdirs();
    }

    @Override
    public void open() throws FileNotFoundException {
        File activeLogPath = new File(baseDir, "logs");
        logChannel = new FileOutputStream(activeLogPath, true);
        logDataOut = newDataOutputStream(logChannel);
    }

    @Override
    public void appendEntry(LogEntry entry) throws IOException {
        entry.writeData(logDataOut);
    }

    @Override
    public void truncateEntriesFrom(long indexInclusive) throws IOException {
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
                if (entry.index() >= indexInclusive) {
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
    public void flush() throws IOException {
        logDataOut.flush();
        logChannel.getFD().sync();
    }

    @Override
    public void writeSnapshot(SnapshotEntry entry) throws IOException {
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

    private ObjectDataOutputStream newDataOutputStream(FileOutputStream fileOutputStream) {
        return new ObjectDataOutputStream(new BufferedOutputStream(fileOutputStream, BUFFER_CAP),
                // TODO: get serialization service from Hazelcast node
                new DefaultSerializationServiceBuilder().build());
    }

    private static InternalSerializationService getSerializationService() {
        // TODO: get serialization service from Hazelcast node
        return new DefaultSerializationServiceBuilder().build();
    }

    @Override
    public void close() throws IOException {
        flush();
        logDataOut.close();
        logChannel.close();
    }
}
