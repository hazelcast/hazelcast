package com.hazelcast.internal.probing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;
import com.hazelcast.internal.probing.ProbeingCycle.Tags;

public class ExampleRootObject implements ProbeSource {

    private volatile Stats foo = new Stats();
    private final String name;
    private volatile long bazCount = 456L;
    private volatile double avgQue = 5.6;
    private final Map<String, Object> bars = new ConcurrentHashMap<String, Object>();

    public ExampleRootObject(String name, ProbeRegistry registry) {
        this.name = name;
        registry.register(this);
    }

    @Override
    public void probeIn(ProbeingCycle cycle) {
        cycle.openContext().tag(INSTANCE_TAG, "foo").tag(TYPE_TAG, "x");
        cycle.probe(foo);
        Tags tags = cycle.openContext().tag(TYPE_TAG, "y");
        for (Entry<String, Object> bar : bars.entrySet()) {
            tags.tag(INSTANCE_TAG, bar.getKey());
            cycle.probe(bar.getValue());
        }
        cycle.openContext().tag(TYPE_TAG, "z").tag(INSTANCE_TAG, name);
        cycle.probe("baz", bazCount);
        cycle.probe("avgQue", avgQue);
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
            count++;
            ratio -= 0.3;
            return Collections.singletonMap("x", "y");
        }
    }

    public static void main(String[] args) throws IOException {
        ProbeRegistryImpl registry = new ProbeRegistryImpl(new ProbeRenderer() {
            
            @Override
            public void render(CharSequence key, long value) {
                System.out.println(key+" value="+value);
            }
        }, 500L);
        registry.register(new ExampleRootObject("hello world", registry));
        
        System.in.read();
    }
}
