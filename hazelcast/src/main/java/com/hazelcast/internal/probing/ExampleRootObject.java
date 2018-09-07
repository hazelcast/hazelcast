package com.hazelcast.internal.probing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.probing.ProbingCycle.Tags;

public class ExampleRootObject implements ProbeSource {

    private volatile Stats foo = new Stats();
    private final String name;
    private volatile long bazCount = 456L;
    private volatile double avgQue = 5.6;
    private final Map<String, Stats> bars = new ConcurrentHashMap<String, Stats>();

    public ExampleRootObject(String name, ProbeRegistry registry) {
        this.name = name;
        registry.register(this);
        for (int i = 0; i < 100; i++) {
            bars.put("i"+i, new Stats());
        }
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.openContext().tag(TAG_INSTANCE, "foo").tag(TAG_TYPE, "x");
        cycle.probe("foo", foo);
        Tags tags = cycle.openContext().tag(TAG_TYPE, "y");
        for (Entry<String, Stats> bar : bars.entrySet()) {
            tags.tag(TAG_INSTANCE, bar.getKey());
            cycle.probe(bar.getValue());
        }
        cycle.openContext().tag(TAG_TYPE, "z").tag(TAG_INSTANCE, name);
        cycle.probe(ProbeLevel.INFO, "baz", bazCount);
        cycle.probe(ProbeLevel.INFO, "avgQue", avgQue);
        cycle.openContext().append("some.").append("flexible.").append("prefix.");
        cycle.probe(ProbeLevel.INFO, "x", 43);
        cycle.probe(ProbeLevel.INFO, "y", 45.5d);
        cycle.openContext().tag(TAG_TYPE, "h");
        cycle.probe(ProbeLevel.INFO, "j", 66);
        cycle.probe(ProbeLevel.INFO, "k", 68);
    }

    private static class Stats {

        @Probe
        private long count = 42;

        @Probe
        private int alsoLong = 3;

        @Probe
        private byte byteAsLong = 1;

        @Probe
        private short sL = 56;

        @Probe
        private double ratio = 4.2d;

        @Probe
        private float floating = 3.2f;

        @Probe
        private Collection<String> set = Arrays.asList("a", "b");

        @Probe
        private Map<String, String> getMap() {
            return Collections.singletonMap("x", "y");
        }
    }

    public static void main(String[] args) throws IOException {
        ProbeRegistryImpl registry = new ProbeRegistryImpl();
        registry.register(new ExampleRootObject("hello world", registry));
        final StringBuilder out = new StringBuilder();
        ProbeRenderer plain = new ProbeRenderer() {

            @Override
            public void render(CharSequence key, long value) {
                out.append(key).append(' ').append(value).append('\n');
            }
        };
        ProbeRenderContext rendering = registry.newRenderingContext();
        rendering.renderAt(ProbeLevel.DEBUG, plain);
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        CompressingProbeRenderer compressing = new CompressingProbeRenderer(out2);
        rendering.renderAt(ProbeLevel.DEBUG, compressing);
        compressing.done();
        int plainLength = out.toString().getBytes().length;
        byte[] compressedBytes = out2.toByteArray();
        int compressedLength = compressedBytes.length;
        final StringBuilder decompressedOut = new StringBuilder();
        CompressingProbeRenderer.decompress(new ByteArrayInputStream(compressedBytes), decompressedOut);
        Set<String> plainSet = toSet(out);
        Set<String> decompressedSet = toSet(decompressedOut);
        System.out.println(out);
        System.out.println("plain bytes: " + plainLength);
        System.out.println("compressed bytes: " + compressedLength);
        System.out.println("compression factor: " + plainLength / (double) compressedLength);
        System.out.println("decompressed equals plain: " + plainSet.equals(decompressedSet));
    }

    private static Set<String> toSet(StringBuilder b) {
        return new TreeSet<String>(Arrays.asList(b.toString().split("\n")));
    }
}
