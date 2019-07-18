package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;

public class OnDiskRaftStateStore implements RaftStateStore {

    private BufferedRaf logRaf;
    private ObjectDataOutput logDataOut;
    private final LogEntryRingBuffer logEntryRingBuffer;

    public OnDiskRaftStateStore(int maxUncommittedEntries) {
        this.logEntryRingBuffer = new LogEntryRingBuffer(maxUncommittedEntries, 0);
    }

    @Override
    public void open() throws IOException {
        logRaf = createFile("raft-log.txt");
        logDataOut = newObjectDataOutput(logRaf);
    }

    @Override
    public void persistInitialMembers(
            RaftEndpoint localMember, Collection<RaftEndpoint> initialMembers
    ) throws IOException {

    }

    @Override
    public void persistTerm(int term, RaftEndpoint votedFor) throws IOException {

    }

    @Override
    public void persistEntry(LogEntry entry) throws IOException {
        logEntryRingBuffer.addEntryOffset(logRaf.filePointer());
        entry.writeData(logDataOut);
    }

    @Override
    public void persistSnapshot(SnapshotEntry snapshot) throws IOException {
        BufferedRaf newRaf = createFile("raft-log.txt_new");
        ObjectDataOutput newDataOut = newObjectDataOutput(newRaf);
        snapshot.writeData(newDataOut);
        if (logEntryRingBuffer.topIndex() <= snapshot.index()) {
            return;
        }
        long newStartOffset = newRaf.filePointer();
        long copyStartOffset = logEntryRingBuffer.getEntryOffset(snapshot.index() + 1);
        logRaf.seek(copyStartOffset);
        logRaf.copyTo(newDataOut);
        logEntryRingBuffer.adjustToNewFile(newStartOffset, snapshot.index());
    }

    @Override
    public void deleteEntriesFrom(long startIndexInclusive) throws IOException {

    }

    @Override
    public void flushLogs() throws IOException {
    }

    @Override
    public void close() throws IOException {

    }

    @Nonnull
    private BufferedRaf createFile(String filename) throws IOException {
        return new BufferedRaf(new RandomAccessFile(filename, "rw"));
    }

    private ObjectDataOutputStream newObjectDataOutput(BufferedRaf bufRaf) throws IOException {
        return new ObjectDataOutputStream(bufRaf.asOutputStream(), getSerializationService());
    }

    // TODO: get serialization service from Hazelcast node
    private InternalSerializationService getSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }
}
