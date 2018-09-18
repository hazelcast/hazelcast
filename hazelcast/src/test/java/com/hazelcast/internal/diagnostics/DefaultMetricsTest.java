package com.hazelcast.internal.diagnostics;

import org.junit.Before;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;

public abstract class DefaultMetricsTest extends AbstractMetricsTest {

    protected HazelcastInstance hz;
    private ProbeRegistry registry;
    private ProbeRenderContext renderContext;

    protected Config configure() {
        return new Config().setProperty(Diagnostics.METRICS_LEVEL.getName(),
                ProbeLevel.INFO.name());
    }

    @Before
    public void setup() {
        hz = createHazelcastInstance(configure());
        registry = getNode(hz).nodeEngine.getProbeRegistry();
        renderContext = registry.newRenderingContext();
        warmUpPartitions(hz);
    }

    @Override
    protected final ProbeRenderContext getRenderContext() {
        return renderContext;
    }

}
