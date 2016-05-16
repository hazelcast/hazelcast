package com.hazelcast.query.impl.getters;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

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
        assertFalse("Portable getter shouln't be cacheable!", getter.isCacheable());
    }

}