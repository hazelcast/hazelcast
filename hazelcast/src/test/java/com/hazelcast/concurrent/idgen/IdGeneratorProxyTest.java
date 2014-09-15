package com.hazelcast.concurrent.idgen;

import static com.hazelcast.mock.IAtomicLongMocks.mockIAtomicLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
        assertFalse( idGenerator.init(-1) );
    }

    @Test
    public void testInitFailsWhenAlreadyInitialized() {
        long first = idGenerator.newId();

        assertFalse( idGenerator.init(10) );

        assertEquals(first + 1, idGenerator.newId());
    }

    @Test
    public void testNewId_withExplicitInit() {

        assertTrue( idGenerator.init(10) );

        long result = idGenerator.newId();
        assertEquals(11, result);
    }

    @Test
    public void testNewId0_withoutExplictInit() {
        long result = idGenerator.newId();
        assertEquals(0, result);
    }

    @Test
    public void testGeneratingMultipleBlocks() {
        for (int k = 0; k < 3 * IdGeneratorProxy.BLOCK_SIZE; k++) {
            assertEquals(k, idGenerator.newId());
        }
    }

	private static void assertNewIdAfterInit(int initialValue) {
	    IdGenerator idGenerator = createIdGenerator();

	    assertTrue( idGenerator.init(initialValue) );

	    assertEquals(initialValue+1, idGenerator.newId());
	}

	private static IdGenerator createIdGenerator() {
		String name = "id-" + UUID.randomUUID().toString();

		IAtomicLong blockGenerator = mockIAtomicLong();

		return new IdGeneratorProxy(blockGenerator, name, null, null);
	}
}
