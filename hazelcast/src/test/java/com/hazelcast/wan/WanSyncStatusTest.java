package com.hazelcast.wan;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.wan.WanSyncStatus.FAILED;
import static com.hazelcast.wan.WanSyncStatus.IN_PROGRESS;
import static com.hazelcast.wan.WanSyncStatus.READY;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanSyncStatusTest {

    @Test
    public void test() {
        assertSame(READY, WanSyncStatus.getByStatus(READY.getStatus()));
        assertSame(IN_PROGRESS, WanSyncStatus.getByStatus(IN_PROGRESS.getStatus()));
        assertSame(FAILED, WanSyncStatus.getByStatus(FAILED.getStatus()));
        assertNull(WanAcknowledgeType.getById(-1));
    }
}