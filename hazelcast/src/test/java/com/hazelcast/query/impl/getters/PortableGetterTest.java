package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PortableGetterTest {

    @Test(expected = IllegalArgumentException.class)
    public void getValue() throws Exception {
        new PortableGetter(null).getValue("input");
    }

    @Test(expected = IllegalArgumentException.class)
    public void getReturnType() throws Exception {
        new PortableGetter(null).getReturnType();
    }

    @Test
    public void isCacheable() throws Exception {
        PortableGetter getter = new PortableGetter(null);
        assertFalse("Portable getter shouldn't be cacheable!", getter.isCacheable());
    }
}
