package com.hazelcast.wan;

import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE;
import static com.hazelcast.config.WanAcknowledgeType.ACK_ON_RECEIPT;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanAcknowledgeTypeTest {

    @Test
    public void test() {
        assertSame(ACK_ON_RECEIPT, WanAcknowledgeType.getById(ACK_ON_RECEIPT.getId()));
        assertSame(ACK_ON_OPERATION_COMPLETE, WanAcknowledgeType.getById(ACK_ON_OPERATION_COMPLETE.getId()));
        assertNull(WanAcknowledgeType.getById(-1));
    }
}
