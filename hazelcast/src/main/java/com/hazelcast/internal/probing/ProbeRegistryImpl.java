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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;

public final class ProbeRegistryImpl implements ProbeRegistry {

    private static final ILogger LOGGER = Logger.getLogger(ProbeRegistryImpl.class);

    private static final Map<Class<?>, ProbeAnnotatedType> PROBE_METADATA = 
            new ConcurrentHashMap<Class<?>, ProbeAnnotatedType>();

    private final Set<ProbeSourceEntry> sources = ConcurrentHashMap.newKeySet();

    @Override
    public void register(ProbeSource source) {
        sources.add(new ProbeSourceEntry(source));
    }

    @Override
    public ProbeRenderContext newRenderingContext() {
        return new ProbingCycleImpl(sources);
    }

    /**
     * Wrapper for a {@link ProbeSource} for the state to keep track of it potential
     * need to be updated before being probed.
     */
    private static final class ProbeSourceEntry {

        final ProbeSource source;
        final Method update;
        final long updateIntervalMs;
        final AtomicLong nextUpdateTimeMs = new AtomicLong();

        public ProbeSourceEntry(ProbeSource source) {
            this.source = source;
            this.update = reprobeFor(source.getClass());
            if (update != null) {
                Reprobe reprobe = update.getAnnotation(Reprobe.class);
                updateIntervalMs = reprobe.unit().toMillis(reprobe.value());
            } else {
                updateIntervalMs = -1L;
            }
        }

        void updateIfNeeded() {
            if (update != null) {
                long next = nextUpdateTimeMs.get();
                if (Clock.currentTimeMillis() > next 
                        && nextUpdateTimeMs.compareAndSet(next, next + updateIntervalMs)) {
                    try {
                        update.invoke(source);
                    } catch (Exception e) {
                        LOGGER.warning("Failed to update source: "
                                + update.getClass().getSimpleName() + "." + update.getName(), e);
                    }
                }
            }
        }

        static Method reprobeFor(Class<?> type) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(Reprobe.class))
                    return m;
            }
            return null;
        }
    }

    private static final class ProbingCycleImpl
            implements ProbingCycle, ProbingCycle.Tags, ProbeRenderContext {

        private final StringBuilder tags = new StringBuilder(128);
        private final Set<ProbeSourceEntry> sources;
        private CharSequence lastTagName;
        private int lastTagValuePosition;
        private boolean endsWithTag = false;

        // render cycle state
        private ProbeRenderer renderer;
        private ProbeLevel level;

        ProbingCycleImpl(Set<ProbeSourceEntry> sources) {
            this.sources = sources;
        }

        @Override
        public void renderAt(ProbeLevel level, ProbeRenderer renderer) {
            this.level = level;
            this.renderer = renderer;
            openContext(); // reset
            for (ProbeSourceEntry e : sources) {
                e.updateIfNeeded();
                e.source.probeIn(this);
            }
        }

        @Override
        public void probe(Object instance) {
            PROBE_METADATA.computeIfAbsent(instance.getClass(), 
                    new Function<Class<?>, ProbeAnnotatedType>() {

                @Override
                public ProbeAnnotatedType apply(Class<?> key) {
                    return new ProbeAnnotatedType(key);
                }
            }).probeIn(this, instance, level);
        }

        @Override
        public void probe(CharSequence prefix, Object instance) {
            append(prefix).append(".");
            probe(instance);
        }

        @Override
        public void probe(CharSequence name, long value) {
            probe(ProbeLevel.MANDATORY, name, value);
        }

        @Override
        public void probe(CharSequence name, double value) {
            probe(ProbeLevel.MANDATORY, name, value);            
        }

        @Override
        public void probe(ProbeLevel level, CharSequence name, long value) {
            if (isProbed(level)) {
                render(name, value);
            }
        }

        @Override
        public void probe(ProbeLevel level, CharSequence name, double value) {
            if (isProbed(level)) {
                render(name, toLong(value));
            }
        }

        @Override
        public Tags openContext() {
            tags.setLength(0);
            lastTagName = null;
            endsWithTag = false;
            return this;
        }

        @Override
        public Tags tag(CharSequence name, CharSequence value) {
            if (name == lastTagName) {
                tags.setLength(lastTagValuePosition);
                appendEscaped(value);
                endsWithTag = true;
                return this;
            }
            appendSpaceToPriorTag();
            tags.append(name).append('=');
            lastTagName = name;
            lastTagValuePosition = tags.length();
            appendEscaped(value);
            endsWithTag = true;
            return this;
        }

        @Override
        public Tags append(CharSequence s) {
            appendSpaceToPriorTag();
            tags.append(s);
            endsWithTag = false;
            return this;
        }

        private void appendSpaceToPriorTag() {
            if (endsWithTag) {
                tags.append(' ');
                endsWithTag = false;
            }
        }

        /**
         * Escapes a user-supplied string values.
         * 
         * Prefixes comma ({@code ","}), space ({@code " "}), equals sign ({@code "="})
         * and backslash ({@code "\"}) with another backslash.
         */
        private void appendEscaped(CharSequence value) {
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
            appendSpaceToPriorTag();
            int len = tags.length();
            appendEscaped(name);
            renderer.render(tags, value);
            tags.setLength(len);
        }

        @Override
        public boolean isProbed(ProbeLevel level) {
            return level.isEnabled(this.level);
        }

        private static long toLong(double value) {
            return round(value * 10000d);
        }
    }

    private static final class ProbeAnnotatedTypeLevel {

        final ProbeLevel level;
        final Map<String, Method> methods = new ConcurrentHashMap<String, Method>();
        final Map<String, Field> longFields = new ConcurrentHashMap<String, Field>();
        final Map<String, Field> doubleFields = new ConcurrentHashMap<String, Field>();
        final Map<String, Field> otherFields = new ConcurrentHashMap<String, Field>();

        ProbeAnnotatedTypeLevel(ProbeLevel level) {
            this.level = level;
        }

        void probeIn(ProbingCycle cycle, Object instance) {
            probeMethods(cycle, instance);
            probeLongFields(cycle, instance);
            probeDoubleFields(cycle, instance);
            probeOtherFields(cycle, instance);
        }

        private void probeOtherFields(ProbingCycle cycle, Object instance) {
            if (!otherFields.isEmpty()) {
                for (Entry<String, Field> f : otherFields.entrySet()) {
                    try {
                        cycle.probe(level, f.getKey(), toLong(f.getValue().get(instance)));
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
                        cycle.probe(level, f.getKey(), f.getValue().getDouble(instance));
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
                        cycle.probe(level, f.getKey(), f.getValue().getLong(instance));
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
                        cycle.probe(level, m.getKey(), toLong(m.getValue().invoke(instance)));
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

    private static final class ProbeAnnotatedType {

        private final ProbeAnnotatedTypeLevel[] levels = 
                new ProbeAnnotatedTypeLevel[ProbeLevel.values().length];

        ProbeAnnotatedType(Class<?> type) {
            init(type);
        }

        void probeIn(ProbingCycle cycle, Object instance, ProbeLevel level) {
            for (int i = 0; i < levels.length; i++) {
                ProbeAnnotatedTypeLevel l = levels[i];
                if (l != null && l.level.isEnabled(level)) {
                    l.probeIn(cycle, instance);
                }
            }
        }

        private void init(Class<?> type) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(Probe.class)) {
                    m.setAccessible(true);
                    Probe probe = m.getAnnotation(Probe.class);
                    String name = probe.name();
                    if (name.isEmpty()) {
                        name = getterIntoProperty(m.getName());
                    }
                    getLevel(probe.level()).methods.put(name, m);
                }
            }
            for (Field f : type.getDeclaredFields()) {
                if (f.isAnnotationPresent(Probe.class)) {
                    f.setAccessible(true);
                    Probe probe = f.getAnnotation(Probe.class);
                    String name = probe.name();
                    ProbeAnnotatedTypeLevel level = getLevel(probe.level());
                    if (name.isEmpty()) {
                        name = f.getName();
                    }
                    Class<?> valueType = f.getType();
                    if (valueType.isPrimitive()) {
                        if (valueType == double.class || valueType == float.class) {
                            level.doubleFields.put(name, f);
                        } else {
                            level.longFields.put(name, f);
                        }
                    } else {
                        level.otherFields.put(name, f);
                    }
                }
            }
            Class<?> superclass = type.getSuperclass();
            if (superclass != null) {
                init(superclass);
            }
        }

        private ProbeAnnotatedTypeLevel getLevel(ProbeLevel level) {
            ProbeAnnotatedTypeLevel res = levels[level.ordinal()];
            if (res == null) {
                res = new ProbeAnnotatedTypeLevel(level);
                levels[level.ordinal()] = res;
            }
            return res;
        }

    }
}
