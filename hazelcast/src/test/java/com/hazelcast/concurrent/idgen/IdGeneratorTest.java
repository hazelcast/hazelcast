package com.hazelcast.concurrent.idgen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IdGeneratorTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
    }

    @Test
    public void testInit() {
        testInit(-1, false, 0);
        testInit(0, true, 1);
        testInit(1, true, 2);
        testInit(10, true, 11);
    }

    private void testInit(int initialValue, boolean expected, long expectedValue) {
        IdGenerator idGenerator = createIdGenerator();

        boolean initialized = idGenerator.init(initialValue);
        assertEquals(expected, initialized);

        long newId = idGenerator.newId();
        assertEquals(expectedValue, newId);
    }

    private IdGenerator createIdGenerator() {
        return hz.getIdGenerator("id-" + UUID.randomUUID().toString());
    }

    @Test
    public void testInitWhenAlreadyInitialized() {
        IdGenerator idGenerator = createIdGenerator();
        long first = idGenerator.newId();

        boolean initialized = idGenerator.init(10);
        assertFalse(initialized);

        long actual = idGenerator.newId();
        assertEquals(first + 1, actual);
    }

    @Test
    public void testNewId_withExplicitInit() {
        IdGenerator idGenerator =createIdGenerator();
        assertTrue(idGenerator.init(10));

        long result = idGenerator.newId();
        assertEquals(11, result);
    }

    @Test
    public void testNewId_withoutExplictInit() {
        IdGenerator idGenerator = createIdGenerator();
        long result = idGenerator.newId();
        assertEquals(0, result);
    }

    @Test
    public void testGeneratingMultipleBlocks() {
        IdGenerator idGenerator = createIdGenerator();

        long expected = 0;
        for (int k = 0; k < 3 * IdGeneratorProxy.BLOCK_SIZE; k++) {
            assertEquals(expected, idGenerator.newId());
            expected++;
        }
    }

    @Test
    public void testDestroy() {
        IdGenerator idGenerator = createIdGenerator();
        String id = idGenerator.getName();
        idGenerator.newId();
        idGenerator.newId();

        idGenerator.destroy();

        IdGenerator newIdGenerator = hz.getIdGenerator(id);
        long actual = newIdGenerator.newId();
        assertEquals(0, actual);
    }
}
