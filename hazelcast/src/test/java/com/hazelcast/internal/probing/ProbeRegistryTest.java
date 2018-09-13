package com.hazelcast.internal.probing;

import static com.hazelcast.internal.probing.Probing.probeAllInstances;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.probing.ProbingCycle.Tagging;
import com.hazelcast.internal.probing.ProbingCycle.Tags;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;

/**
 * While there are specific tests for the basic {@link Probe} annotated field
 * and method mapping this class focuses on the more advanced scenarios.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeRegistryTest extends ProbingTest implements ProbeSource {

    @Before
    public void setUp() {
        registry.register(this);
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        LevelBean a = new LevelBean("a");
        LevelBean b = new LevelBean("b");
        LevelBean c = new LevelBean("");
        // context should be clean - no openContext required
        cycle.probe("foo", a); // should be context neutral
        // context should still be clean, again no openContext
        cycle.probe("bar", b);
        // and again
        cycle.probe("baz", c);
        Map<String, LevelBean> map = new HashMap<String, LevelBean>();
        map.put("x", a);
        map.put("y", b);
        map.put("z", c);
        probeAllInstances(cycle, "map", map);
        cycle.openContext(); // needed to reset context
        cycle.probe(this);
    }

    private static final class LevelBean implements Tagging {

        private final String name;

        public LevelBean(String name) {
            this.name = name;
        }

        @Probe(level = ProbeLevel.MANDATORY)
        long mandatory = 1;

        @Probe(level = ProbeLevel.INFO)
        long info = 2;

        @Probe(level = ProbeLevel.DEBUG)
        long debug = 3;

        @Override
        public void tagIn(Tags context) {
            if (!name.isEmpty()) {
                context.tag(TAG_INSTANCE, name);
            }
        }
    }

    @Test
    public void onlyProbesOfEnabledLevelsAreRendered() {
        setLevel(ProbeLevel.MANDATORY);
        assertProbeCount(6);
        assertProbes("mandatory", 1);
        setLevel(ProbeLevel.INFO);
        assertProbeCount(13);
        assertProbes("mandatory", 1);
        assertProbes("info", 2);
        setLevel(ProbeLevel.DEBUG);
        assertProbeCount(19);
        assertProbes("mandatory", 1);
        assertProbes("info", 2);
        assertProbes("debug", 3);
    }

    private void assertProbes(String name, long value) {
        String i = TAG_INSTANCE;
        String t = TAG_TYPE;
        assertProbed(i + "=a foo." + name, value);
        assertProbed(i + "=b bar." + name, value);
        assertProbed("baz." + name, value);
        assertProbed(t + "=map " + i + "=a " + name, value);
        assertProbed(t + "=map " + i + "=b " + name, value);
        assertProbed(t + "=map " + i + "=z " + name, value);
    }

    @Probe
    private int updates = 0;

    @ReprobeCycle(value = 500, unit = TimeUnit.MILLISECONDS)
    private void update() {
        updates++;
    }

    @Test
    public void reprobingOccursInSpecifiedCycleTime() {
        assertProbed("updates", 1);
        assertProbed("updates", 1);
        sleepAtLeastMillis(500L);
        assertProbed("updates", 2);
        assertProbed("updates", 2);
    }

}
