package com.hazelcast.concurrent.lock;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalLockNamespaceTest extends HazelcastTestSupport {

    @Test
    public void givenSomeName_whenOtherNameIsEqual_thenNamespacesAreEqualToo() {
        String name1 = randomName();
        String name2 = new String(name1);

        InternalLockNamespace ns1 = new InternalLockNamespace(name1);
        InternalLockNamespace ns2 = new InternalLockNamespace(name2);

        assertEquals(ns1, ns2);
    }

    @Test
    public void givenSomeName_whenOtherNameIsEqual_thenHashCodeIsSame() {
        String name1 = randomName();
        String name2 = new String(name1);

        InternalLockNamespace ns1 = new InternalLockNamespace(name1);
        InternalLockNamespace ns2 = new InternalLockNamespace(name2);

        assertEquals(ns1.hashCode(), ns2.hashCode());
    }

    @Test
    public void givenSomeName_whenOtherNameIsNotEqual_thenNamespacesAreNotEqual() {
        String name1 = randomName();
        String name2 = randomName();

        InternalLockNamespace ns1 = new InternalLockNamespace(name1);
        InternalLockNamespace ns2 = new InternalLockNamespace(name2);

        assertNotEquals(ns1, ns2);
    }

    @Test
    public void givenNameIsNull_whenOtherNameIsNullToo_thenNamespacesAreEqual() {
        InternalLockNamespace ns1 = new InternalLockNamespace(null);
        InternalLockNamespace ns2 = new InternalLockNamespace(null);

        assertEquals(ns1, ns2);
    }

    @Test
    public void givenSomeName_whenOtherNameIsNull_thenNamespacesAreNotEqual() {
        String name1 = randomName();

        InternalLockNamespace ns1 = new InternalLockNamespace(name1);
        InternalLockNamespace ns2 = new InternalLockNamespace(null);

        assertNotEquals(ns1, ns2);
    }

    @Test
    public void givenNameIsNull_whenOtherNameIsNotNull_thenNamespacesAreNotEqual() {
        String name1 = randomName();

        InternalLockNamespace ns1 = new InternalLockNamespace(name1);
        InternalLockNamespace ns2 = new InternalLockNamespace(null);

        assertNotEquals(ns1, ns2);
    }



}
