package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.logging.Logger;

import static com.hazelcast.test.HazelcastTestSupport.randomString;

/**
 * Simple extension of {@link SnapshotContext} that replaces usage that would
 * have otherwise been {@link org.mockito.Mockito#mock(Class)}'ed - helps
 * avoid non-deterministic Mockito-related failures on certain JVMs like Zing
 */
public class MockSnapshotContext extends SnapshotContext {
    private String currentMapName;
    private long currentSnapshotId;

    public MockSnapshotContext() {
        super(Logger.getLogger(MockSnapshotContext.class), randomString(), 0L, ProcessingGuarantee.NONE);
    }

    @Override
    public String currentMapName() {
        return currentMapName;
    }

    public void setCurrentMapName(String currentMapName) {
        this.currentMapName = currentMapName;
    }

    @Override
    public long currentSnapshotId() {
        return currentSnapshotId;
    }

    public void setCurrentSnapshotId(long currentSnapshotId) {
        this.currentSnapshotId = currentSnapshotId;
    }
}
