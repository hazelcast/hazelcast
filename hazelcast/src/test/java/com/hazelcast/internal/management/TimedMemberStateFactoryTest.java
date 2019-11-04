package com.hazelcast.internal.management;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.hotrestart.NoOpHotRestartService;
import com.hazelcast.internal.hotrestart.NoopInternalHotRestartService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Test;

import java.net.UnknownHostException;

import static com.hazelcast.instance.TestNodeContext.mockNs;
import static com.hazelcast.test.HazelcastTestSupport.getHazelcastInstanceImpl;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimedMemberStateFactoryTest
        extends HazelcastTestSupport {

    HazelcastInstance instance;

    private TimedMemberStateFactory createTimedMemberStateFactory(String xmlConfigRelativeFileName) {
        Config config = new ClasspathXmlConfig("com/hazelcast/internal/management/" + xmlConfigRelativeFileName);
        //            NodeExtension nodeExtension = mock(NodeExtension.class);
        //            when(nodeExtension.getHotRestartService()).thenReturn(new NoOpHotRestartService());
        //            when(nodeExtension.getInternalHotRestartService()).thenReturn(new NoopInternalHotRestartService());
        //            NodeContext context = new TestNodeContext(new Address("127.0.0.1", 5000), nodeExtension, mockNs());
        //            instance = HazelcastInstanceFactory.newHazelcastInstance(config, "name", context);
        instance = createHazelcastInstance(config);
        return new TimedMemberStateFactory(getHazelcastInstanceImpl(instance));
    }

    @After
    public void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    @Test
    public void optaneMemorySupport_explicitlyEnabled() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-enabled.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertTrue(actual.isNativeMemoryEnabled());
    }

    @Test
    public void optaneMemorySupport_implicitlyDisabled() {
        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("empty-config.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        assertFalse(actual.isNativeMemoryEnabled());
    }

    @Test
    public void nativeMemoryEnabledForMap_withPositiveSize() {

        TimedMemberStateFactory memberStateFactory = createTimedMemberStateFactory("native-memory-for-map.xml");
        TimedMemberState actual = memberStateFactory.createTimedMemberState();

        instance.getMap("myMap");

        assertTrue(actual.getMemberState().getLocalMapStats("myMap").isNativeMemoryUsed());
    }
}
