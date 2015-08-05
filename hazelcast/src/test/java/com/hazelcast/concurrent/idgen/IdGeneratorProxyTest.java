package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.mock.IAtomicLongMocks.mockIAtomicLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IdGeneratorProxyTest {

    private IdGenerator idGenerator = createIdGenerator();

    @Test
    public void testInitIncrements() {
        assertNewIdAfterInit(0);
        assertNewIdAfterInit(1);
        assertNewIdAfterInit(10);
    }

    @Test
    public void testInitFailsOnNegativeValues() {
        assertFalse(idGenerator.init(-1));
    }

    private static void assertNewIdAfterInit(int initialValue) {
        IdGenerator idGenerator = createIdGenerator();

        assertTrue(idGenerator.init(initialValue));

        assertEquals(initialValue + 1, idGenerator.newId());
    }

    private static IdGenerator createIdGenerator() {
        String name = "id-" + UUID.randomUUID().toString();

        IAtomicLong blockGenerator = mockIAtomicLong();

        return new IdGeneratorProxy(blockGenerator, name, null, null);
    }
}
