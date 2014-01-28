package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        testInit(0, false, 0);
        testInit(-1, false, 0);
        testInit(1, true, 2);
        testInit(10, true, 11);
    }

    private void testInit(int initialValue, boolean expected, long expectedValue) {
        IdGenerator idGenerator = hz.getIdGenerator("id-" + UUID.randomUUID().toString());
        boolean result = idGenerator.init(initialValue);
        assertEquals(expected, result);

        long newId = idGenerator.newId();
        assertEquals(expectedValue, newId);
    }

    @Test
    public void testInitWhenAlreadyInitialized(){
        IdGenerator idGenerator = hz.getIdGenerator("id-" + UUID.randomUUID().toString());
        idGenerator.newId();

        testInit(10,false,2);
    }

    @Test
    public void testNewId_withExplicitInit() {
        IdGenerator idGenerator = hz.getIdGenerator("testNewId_withExplicitInit");
        assertTrue(idGenerator.init(10));

        long result = idGenerator.newId();
        assertEquals(11, result);
    }

    @Test
    public void testNewId() {
        IdGenerator idGenerator = hz.getIdGenerator("test");
        long result = idGenerator.newId();
        assertEquals(0, result);
    }

    @Test
    public void testGeneratingMultipleBlocks() {
        IdGenerator idGenerator = hz.getIdGenerator("test");

        long expected = 0;
        for (int k = 0; k < 10 * IdGeneratorProxy.BLOCK_SIZE; k++) {
            assertEquals(expected, idGenerator.newId());
            expected++;
        }
    }
}
