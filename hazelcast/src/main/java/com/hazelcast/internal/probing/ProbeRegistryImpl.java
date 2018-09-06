package com.hazelcast.internal.probing;

import static com.hazelcast.util.StringUtil.getterIntoProperty;
import static java.lang.Math.round;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;

public final class ProbeRegistryImpl implements ProbeRegistry {

    private final Map<Class<?>, ProbeAnnotatedType> metadata = 
            new ConcurrentHashMap<Class<?>, ProbeAnnotatedType>();

    private final Set<ProbeSource> sources = ConcurrentHashMap.newKeySet();

    @Override
    public void register(ProbeSource source) {
        sources.add(source);
    }

    @Override
    public void renderTo(ProbeRenderer renderer) {
        //? make this synchronous so same cycle instance can be reused?
        ProbingCycleImpl cycle = new ProbingCycleImpl(renderer, metadata);
        for (ProbeSource s : sources) {
            s.probeIn(cycle);
        }
    }

    private static final class ProbingCycleImpl implements ProbingCycle, ProbingCycle.Tags {

        private final StringBuilder tags = new StringBuilder(128);
        private final ProbeRenderer renderer;
        private final Map<Class<?>, ProbeAnnotatedType> metadata;
        private CharSequence lastTagName;
        private int lastTagValuePosition;

        public ProbingCycleImpl(ProbeRenderer renderer,
                Map<Class<?>, ProbeAnnotatedType> metadata) {
            this.renderer = renderer;
            this.metadata = metadata;
        }

        @Override
        public void probe(Object instance) {
            metadata.computeIfAbsent(instance.getClass(), new Function<Class<?>, ProbeAnnotatedType>() {

                @Override
                public ProbeAnnotatedType apply(Class<?> key) {
                    return new ProbeAnnotatedType(key);
                }
            }).probeIn(this, instance);
        }

        @Override
        public void probe(CharSequence prefix, Object instance) {
            appendName(prefix);
            tags.append('.');
            probe(instance);
        }

        @Override
        public void probe(CharSequence name, long value) {
            render(name, value);
        }

        @Override
        public void probe(CharSequence name, double value) {
            render(name, toLong(value));
        }

        @Override
        public void probe(CharSequence name, int value) {
            probe(name, (long) value);
        }

        @Override
        public void probe(CharSequence name, float value) {
            probe(name, (double) value);
        }

        @Override
        public Tags openContext() {
            tags.setLength(0);
            lastTagName = null;
            return this;
        }

        @Override
        public Tags tag(CharSequence name, CharSequence value) {
            if (name == lastTagName) {
                tags.setLength(lastTagValuePosition);
                appendValue(value);
                return this;
            }
            appendName(name);
            lastTagName = name;
            lastTagValuePosition = tags.length();
            appendValue(value);
            return this;
        }

        private void appendName(CharSequence name) {
            if (tags.length() > 0) {
                tags.append(' ');
            }
            tags.append(name).append('=');
        }

        /**
         * Escapes a user-supplied string values.
         * 
         * Prefixes comma ({@code ","}), space ({@code " "}), equals sign ({@code "="})
         * and backslash ({@code "\"}) with another backslash.
         */
        private void appendValue(CharSequence value) {
            int len = value.length();
            for (int i = 0; i < len; i++) {
                char c = value.charAt(i);
                if (c == ',' || c == ' ' || c == '\\' || c == '=') {
                    tags.append('\\');
                }
                tags.append(c);
            }
        }

        private void render(CharSequence name, long value) {
            int len = tags.length();
            appendName("name");
            appendValue(name);
            renderer.render(tags, value);
            tags.setLength(len);
        }

        private static long toLong(double value) {
            return round(value * 10000d);
        }
    }

    private static final class ProbeAnnotatedType {

        private final Map<String, Method> methods = new ConcurrentHashMap<String, Method>();
        private final Map<String, Field> longFields = new ConcurrentHashMap<String, Field>();
        private final Map<String, Field> doubleFields = new ConcurrentHashMap<String, Field>();
        private final Map<String, Field> otherFields = new ConcurrentHashMap<String, Field>();

        public ProbeAnnotatedType(Class<?> type) {
            init(type);
        }

        private void init(Class<?> type) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(Probe.class)) {
                    m.setAccessible(true);
                    String name = m.getAnnotation(Probe.class).name();
                    if (name.isEmpty()) {
                        name = getterIntoProperty(m.getName());
                    }
                    methods.put(name, m);
                }
            }
            for (Field f : type.getDeclaredFields()) {
                if (f.isAnnotationPresent(Probe.class)) {
                    f.setAccessible(true);
                    String name = f.getAnnotation(Probe.class).name();
                    if (name.isEmpty()) {
                        name = f.getName();
                    }
                    Class<?> valueType = f.getType();
                    if (valueType.isPrimitive()) {
                        if (valueType == double.class || valueType == float.class) {
                            doubleFields.put(name, f);
                        } else {
                            longFields.put(name, f);
                        }
                    } else {
                        otherFields.put(name, f);
                    }
                }
            }
            Class<?> superclass = type.getSuperclass();
            if (superclass != null) {
                init(superclass);
            }
        }

        public void probeIn(ProbingCycle cycle, Object instance) {
            probeMethods(cycle, instance);
            probeLongFields(cycle, instance);
            probeDoubleFields(cycle, instance);
            probeOtherFields(cycle, instance);
        }

        private void probeOtherFields(ProbingCycle cycle, Object instance) {
            if (!otherFields.isEmpty()) {
                for (Entry<String, Field> f : otherFields.entrySet()) {
                    try {
                        cycle.probe(f.getKey(), toLong(f.getValue().get(instance)));
                    } catch (Exception e) {
                        // ignore it (or write -1?)
                    }
                }
            }
        }

        private void probeDoubleFields(ProbingCycle cycle, Object instance) {
            if (!doubleFields.isEmpty()) {
                for (Entry<String, Field> f : doubleFields.entrySet()) {
                    try {
                        cycle.probe(f.getKey(), f.getValue().getDouble(instance));
                    } catch (Exception e) {
                        // ignore it (or write -1?)
                    }
                }
            }
        }

        private void probeLongFields(ProbingCycle cycle, Object instance) {
            if (!longFields.isEmpty()) {
                for (Entry<String, Field> f : longFields.entrySet()) {
                    try {
                        cycle.probe(f.getKey(), f.getValue().getLong(instance));
                    } catch (Exception e) {
                        // ignore it (or write -1?)
                    }
                }
            }
        }

        private void probeMethods(ProbingCycle cycle, Object instance) {
            if (!methods.isEmpty()) {
                for (Entry<String, Method> m : methods.entrySet()) {
                    try {
                        cycle.probe(m.getKey(), toLong(m.getValue().invoke(instance)));
                    } catch (Exception e) {
                        // ignore it (or write -1?)
                    }
                }
            }
        }

        private static long toLong(Object value) {
            if (value == null) {
                return -1L;
            }
            if (value instanceof Number) {
                Class<?> type = value.getClass();
                if (type == Float.class || type == Double.class) {
                    return toLong(((Number) value).doubleValue());
                }
                return ((Number) value).longValue();
            }
            if (value instanceof Collection) {
                return ((Collection<?>) value).size();
            }
            if (value instanceof Map) {
                return ((Map<?, ?>) value).size();
            }
            if (value instanceof Counter) {
                return ((Counter) value).get();
            }
            if (value instanceof Semaphore) {
                return ((Semaphore) value).availablePermits();
            }
            throw new UnsupportedOperationException(
                    "It is not known how to convert a value of type "
                            + value.getClass().getSimpleName() + " to primitive long.");
        }
    }
}
