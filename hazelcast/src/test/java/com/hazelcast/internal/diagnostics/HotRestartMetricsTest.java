package com.hazelcast.internal.diagnostics;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.BackupTaskState;
import com.hazelcast.hotrestart.BackupTaskStatus;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.hotrestart.NoOpHotRestartService;
import com.hazelcast.hotrestart.NoopInternalHotRestartService;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.DefaultNodeExtension;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;
import com.hazelcast.test.HazelcastParallelClassRunner;

@RunWith(HazelcastParallelClassRunner.class)
public class HotRestartMetricsTest extends AbstractMetricsTest {

    private HazelcastInstance hz;
    private ProbeRenderContext renderContext;

    @Before
    public void setUp() {
        Config config = new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name());
        hz = HazelcastInstanceFactory.newHazelcastInstance(config, randomName(),
                new HotRestartMockingNodeContext());
        renderContext = getNode(hz).nodeEngine.getProbeRegistry().newRenderingContext();
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.shutdownAll();
    }

    @Override
    protected ProbeRenderContext getRenderContext() {
        return renderContext;
    }

    @Test
    public void hotRestartStatus() {
        assertHasStatsEventually(4, "hotRestart.");
        assertHasStatsEventually(1, "instance=foo hotRestart.");
    }

    @Test
    public void hotBackupStatus() {
        assertHasStatsEventually(4, "hotBackup.");
    }

    private static class HotRestartMockingNodeContext extends DefaultNodeContext {

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new HotRestartMockingNodeExtension(node);
        }
    }

    private static class HotRestartMockingNodeExtension extends DefaultNodeExtension {

        public HotRestartMockingNodeExtension(Node node) {
            super(node);
        }

        @Override
        public InternalHotRestartService getInternalHotRestartService() {
            return new FakeInternalHotRestartService();
        }

        @Override
        public HotRestartService getHotRestartService() {
            return new FakeHotRestartService();
        }
    }

    private static class FakeHotRestartService extends NoOpHotRestartService {
        @Override
        public BackupTaskStatus getBackupTaskStatus() {
            return new BackupTaskStatus(BackupTaskState.IN_PROGRESS, 50, 100);
        }

        @Override
        public boolean isHotBackupEnabled() {
            return true;
        }
    }

    private static class FakeInternalHotRestartService extends NoopInternalHotRestartService {

        @Override
        public ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus() {
            return new ClusterHotRestartStatusDTO(
                    HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY,
                    ClusterHotRestartStatus.IN_PROGRESS, 42, 13,
                    Collections.singletonMap("foo", MemberHotRestartStatus.LOAD_IN_PROGRESS));
        }
    }
}
