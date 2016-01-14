package com.hazelcast.concurrent.idgen;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class IdGeneratorAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IdGenerator idGenerator;

    @Before
    public void setup() {
        instances = newInstances();
        idGenerator = newInstance();
    }

    protected IdGenerator newInstance() {
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String name = generateKeyOwnedBy(target);
        return local.getIdGenerator(name);
    }

    protected abstract HazelcastInstance[] newInstances();

    @Test
    public void testInit() {
        testInit(-1, false, 0);
        testInit(0, true, 1);
        testInit(1, true, 2);
        testInit(10, true, 11);
    }

    private void testInit(int initialValue, boolean expected, long expectedValue) {
        IdGenerator idGenerator = newInstance();

        boolean initialized = idGenerator.init(initialValue);
        assertEquals(expected, initialized);

        long newId = idGenerator.newId();
        assertEquals(expectedValue, newId);
    }

    @Test
    public void testInitWhenAlreadyInitialized() {
        long first = idGenerator.newId();

        boolean initialized = idGenerator.init(10);
        assertFalse(initialized);

        long actual = idGenerator.newId();
        assertEquals(first + 1, actual);
    }

    @Test
    public void testNewId_withExplicitInit() {
        assertTrue(idGenerator.init(10));

        long result = idGenerator.newId();
        assertEquals(11, result);
    }

    @Test
    public void testNewId_withoutExplictInit() {
        long result = idGenerator.newId();
        assertEquals(0, result);
    }

    @Test
    public void testGeneratingMultipleBlocks() {
        long expected = 0;
        for (int k = 0; k < 3 * IdGeneratorImpl.BLOCK_SIZE; k++) {
            assertEquals(expected, idGenerator.newId());
            expected++;
        }
    }

    @Test
    public void testDestroy() {
        String id = idGenerator.getName();
        idGenerator.newId();
        idGenerator.newId();

        idGenerator.destroy();

        IdGenerator newIdGenerator = instances[0].getIdGenerator(id);
        long actual = newIdGenerator.newId();
        assertEquals(0, actual);
    }
}
