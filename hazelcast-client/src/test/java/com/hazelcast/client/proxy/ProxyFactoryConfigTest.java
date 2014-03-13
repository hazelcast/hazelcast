package com.hazelcast.client.proxy;

import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ProxyFactoryConfigTest {

    @Test
    public void ProxyFactoryConfig_NullTest() {
        ProxyFactoryConfig p = new ProxyFactoryConfig();
        assertNull(p.getClassName());
        assertNull(p.getService());
    }

    @Test
    public void ProxyFactoryConfig_Test() {
        ProxyFactoryConfig p = new ProxyFactoryConfig("class", "service");
        assertEquals("class", p.getClassName());
        assertEquals("service", p.getService());
    }

    @Test
    public void ProxyFactoryConfig_setTest() {
        ProxyFactoryConfig p = new ProxyFactoryConfig();
        p.setClassName("class");
        p.setService("service");
        assertEquals("class", p.getClassName());
        assertEquals("service", p.getService());
    }

}
