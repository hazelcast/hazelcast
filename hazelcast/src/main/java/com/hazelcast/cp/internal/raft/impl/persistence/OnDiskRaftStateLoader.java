package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.exception.LogValidationException;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.cp.internal.raft.impl.persistence.OnDiskRaftStateStore.RAFT_LOG_PREFIX;
import static com.hazelcast.cp.internal.raft.impl.persistence.OnDiskRaftStateStore.getSerializationService;

public class OnDiskRaftStateLoader implements RaftStateLoader {

    private static final LogEntry[] EMPTY_LOG_ENTRY_ARRAY = new LogEntry[0];

    private enum LoadMode { FULL, JUST_TOP_INDEX }

    @Nonnull @Override
    public RestoredRaftState load() throws IOException {
        String[] logFiles = new File(".").list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(RAFT_LOG_PREFIX);
            }
        });
        if (logFiles == null) {
            throw new IOException("Error opening the Raft log directory");
        }
        if (logFiles.length == 0) {
            return new RestoredRaftState(null, null, 0, null, null, EMPTY_LOG_ENTRY_ARRAY);
        }
        Arrays.sort(logFiles);
        Collections.reverse(Arrays.asList(logFiles));
        RestoredLogFile mostRecentByName = loadFile(logFiles[0], LoadMode.FULL);
        RestoredLogFile mostRecentByTopIndex = mostRecentByName;
        for (int i = 1; i < logFiles.length; i++) {
            String fname = logFiles[i];
            RestoredLogFile rlf = loadFile(fname, LoadMode.JUST_TOP_INDEX);
            if (rlf.topIndex() > mostRecentByTopIndex.topIndex()) {
                mostRecentByTopIndex = rlf;
            }
        }
        RestoredLogFile resolved = mostRecentByName == mostRecentByTopIndex ?
                mostRecentByName : loadFile(mostRecentByTopIndex.filename(), LoadMode.FULL);
        return new RestoredRaftState(
                null, null, 0, null, resolved.snapshotEntry(), resolved.entries()
        );
    }

    private RestoredLogFile loadFile(String fname, LoadMode loadMode) throws IOException {
        BufferedRaf raf = new BufferedRaf(new RandomAccessFile(fname, "r"));
        try {
            ObjectDataInputStream in = raf.asObjectDataInputStream(getSerializationService());
            List<LogEntry> entries = new ArrayList<LogEntry>();
            long topIndex = 0;
            while (true) {
                LogEntry entry;
                long entryOffset = raf.filePointer();
                try {
                    entry = in.readObject();
                } catch (Exception e) {
                    raf.setLength(entryOffset);
                    break;
                }
                if (entry.index() < topIndex) {
                    throw new LogValidationException(String.format(
                            "Log entries not ordered by their index. Top index so far: %,d, now read %,d. File is %s",
                            topIndex, entry.index(), fname)
                    );
                }
                if (entry instanceof SnapshotEntry && topIndex != 0) {
                    throw new LogValidationException("Snapshot entry not at the start of the file " + fname);
                }
                topIndex = entry.index();
                if (loadMode == LoadMode.FULL) {
                    entries.add(entry);
                }
            }
            if (entries.isEmpty()) {
                return new RestoredLogFile(fname, null, EMPTY_LOG_ENTRY_ARRAY, topIndex);
            }
            SnapshotEntry snapshot =
                    entries.get(0) instanceof SnapshotEntry ? (SnapshotEntry) entries.remove(0)
                            : null;
            return new RestoredLogFile(fname, snapshot, entries.toArray(EMPTY_LOG_ENTRY_ARRAY), topIndex);
        } finally {
            raf.close();
        }
    }
}
