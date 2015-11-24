package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapQuorumLiteMemberTest extends HazelcastTestSupport {


    private TestHazelcastInstanceFactory factory;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(3);
    }

    @Test(expected = QuorumException.class)
    public void test_readQuorumNotSatisfied_withLiteMembers() {
        final Config config = craeteConfig("r", QuorumType.READ, 3, false);
        final Config liteConfig = craeteConfig("r", QuorumType.READ, 3, true);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("r").keySet();
    }

    @Test
    public void test_readQuorumSatisfied_withLiteMembers() {
        final Config config = craeteConfig("r", QuorumType.READ, 2, false);
        final Config liteConfig = craeteConfig("r", QuorumType.READ, 2, true);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("r").keySet();
    }

    @Test(expected = QuorumException.class)
    public void test_readReadWriteQuorumNotSatisfied_withLiteMembers() {
        final Config config = craeteConfig("rw", QuorumType.READ_WRITE, 3, false);
        final Config liteConfig = craeteConfig("rw", QuorumType.READ_WRITE, 3, true);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("rw").keySet();
    }

    @Test
    public void test_readReadWriteQuorumSatisfied_withLiteMembers() {
        final Config config = craeteConfig("rw", QuorumType.READ_WRITE, 2, false);
        final Config liteConfig = craeteConfig("rw", QuorumType.READ_WRITE, 2, true);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("rw").keySet();
    }

    @Test(expected = QuorumException.class)
    public void test_readWriteQuorumNotSatisfied_withLiteMembers() {
        final Config config = craeteConfig("w", QuorumType.WRITE, 3, false);
        final Config liteConfig = craeteConfig("w", QuorumType.WRITE, 3, true);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("w").put(0, 0);
    }

    @Test
    public void test_readWriteQuorumSatisfied_withLiteMembers() {
        final Config config = craeteConfig("w", QuorumType.WRITE, 2, false);
        final Config liteConfig = craeteConfig("w", QuorumType.WRITE, 2, true);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(liteConfig);

        instance.getMap("w").put(0, 0);
    }

    private Config craeteConfig(final String name, final QuorumType type, final int size, final boolean liteMember) {
        final QuorumConfig quorumConfig = new QuorumConfig().setName(name).setType(type).setEnabled(true).setSize(size);
        final MapConfig mapConfig = new MapConfig(name);
        mapConfig.setQuorumName(name);
        Config config = new Config();
        config.addQuorumConfig(quorumConfig);
        config.addMapConfig(mapConfig);
        config.setLiteMember(liteMember);
        return config;
    }

}
