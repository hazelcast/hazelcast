package com.hazelcast.internal.probing;

import static com.hazelcast.util.StringUtil.getterIntoProperty;
import static java.lang.Math.round;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final Object[] EMPTY_ARGS = new Object[0];

    private static final Map<Class<?>, ProbeAnnotatedType> PROBE_METADATA = 
            new ConcurrentHashMap<Class<?>, ProbeAnnotatedType>();

    private final Set<ProbeSourceEntry> sources = ConcurrentHashMap.newKeySet();

    @Override
    public void register(ProbeSource source) {
        for (ProbeSourceEntry e : sources) {
            if (e.source.equals(source)) {
                LOGGER.info("Probe source registered more then once: "
                        + source.getClass().getSimpleName());
                return; // avoid adding the very same instance more then once
            }
        }
        sources.add(new ProbeSourceEntry(source));
    }

    @Override
    public ProbeRenderContext newRenderingContext() {
        return new ProbingCycleImpl(sources);
    }

    static long updateInterval(ReprobeCycle reprobe) {
        return reprobe == null ? -1L : reprobe.unit().toMillis(reprobe.value());
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
                updateIntervalMs = updateInterval(update.getAnnotation(ReprobeCycle.class));
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
                        update.invoke(source, EMPTY_ARGS);
                    } catch (Exception e) {
                        LOGGER.warning("Failed to update source: "
                                + update.getDeclaringClass().getSimpleName() + "."
                                + update.getName(), e);
                    }
                }
            }
        }

        static Method reprobeFor(Class<?> type) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(ReprobeCycle.class)) {
                    m.setAccessible(true);
                    return m;
                }
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
            for (ProbeSourceEntry entry : sources) {
                entry.updateIfNeeded();
                try {
                    openContext(); // just to be sure it is done even if ommitted in the source
                    entry.source.probeIn(this);
                } catch (Exception e) {
                    LOGGER.warning("Exception while probing source "
                            + entry.source.getClass().getSimpleName(), e);
                }
            }
        }

        @Override
        public void probe(final ProbeLevel level, Object instance, final String... methods) {
            if (!isProbed(level)) {
                return;
            }
            PROBE_METADATA.computeIfAbsent(instance.getClass(), new Function<Class<?>, ProbeAnnotatedType>() {

                @Override
                public ProbeAnnotatedType apply(Class<?> type) {
                    return new ProbeAnnotatedType(type, level, methods);
                }
            }).probeIn(this, instance, this.level);
        }

        @Override
        public void probe(Object instance) {
            PROBE_METADATA.computeIfAbsent(instance.getClass(), 
                    new Function<Class<?>, ProbeAnnotatedType>() {

                @Override
                public ProbeAnnotatedType apply(Class<?> type) {
                    return new ProbeAnnotatedType(type);
                }
            }).probeIn(this, instance, level);
        }

        @Override
        public void probe(CharSequence prefix, Object instance) {
            int len = tags.length();
            prefix(prefix);
            probe(instance);
            tags.setLength(len);
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
        public void probe(CharSequence name, boolean value) {
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
            probe(level, name, toLong(value));
        }

        @Override
        public void probe(ProbeLevel level, CharSequence name, boolean value) {
            probe(level, name, value ? 1 : 0);
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
            appendEscaped(s); // this might contain user supplied values
            endsWithTag = false;
            return this;
        }

        @Override
        public Tags prefix(CharSequence prefix) {
            if (prefix.length() > 0) {
                return append(prefix).append(".");
            }
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
            ProbeRegistryImpl.appendEscaped(tags, value);
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
    }

    /**
     * Holds the state for a specific {@link ProbeLevel} for a specific
     * {@link Class} type.
     * 
     * The unconventional usage of array pairs to model "maps" that requires
     * cumbersome initialization code (runs once) has two main goals: consume as
     * little memory as possible while providing the possibility to iterate the
     * "entries" (runs often) without causing creation of garbage objects while
     * keeping the type inspection in the initialization phase that runs once.
     */
    private static final class ProbeAnnotatedTypeLevel {

        final ProbeLevel level;
        final String[] methodNames;
        final String[] longFieldNames;
        final String[] doubleFieldNames;
        final String[] booleanFieldNames;
        final String[] otherFieldNames;
        final Method[] methods;
        final Field[] longFields;
        final Field[] doubleFields;
        final Field[] booleanFields;
        final Field[] otherFields;

        static ProbeAnnotatedTypeLevel createIfNeeded(ProbeLevel level, List<Method> probedMethods,
                List<Field> probedFields) {
            int methodCount = countMethodProbesWith(level, probedMethods);
            int longFieldCount = countFieldProbesWith(level, probedFields, long.class, int.class,
                    short.class, char.class, byte.class);
            int doubleFieldCount = countFieldProbesWith(level, probedFields, double.class,
                    float.class);
            int booleanFieldCount = countFieldProbesWith(level, probedFields, boolean.class);
            int otherFieldCount = countFieldProbesWith(level, probedFields) - longFieldCount
                    - doubleFieldCount - booleanFieldCount;
            if (methodCount + longFieldCount + doubleFieldCount + booleanFieldCount
                    + otherFieldCount == 0) {
                return null;
            }
            return new ProbeAnnotatedTypeLevel(level, probedMethods, probedFields, methodCount,
                    longFieldCount, doubleFieldCount, booleanFieldCount, otherFieldCount);
        }

        ProbeAnnotatedTypeLevel(ProbeLevel level, List<Method> probedMethods,
                List<Field> probedFields, int methodCount, int longFieldCount, int doubleFieldCount,
                int booleanFieldCount, int otherFieldCount) {
            this.level = level;
            this.methods = methodCount == 0 ? null : new Method[methodCount];
            this.methodNames = methodCount == 0 ? null : new String[methodCount];
            initMethodProbes(level, probedMethods);
            this.longFields = longFieldCount == 0 ? null : new Field[longFieldCount];
            this.longFieldNames = longFieldCount == 0 ? null : new String[longFieldCount];
            this.doubleFields = doubleFieldCount == 0 ? null : new Field[doubleFieldCount];
            this.doubleFieldNames = doubleFieldCount == 0 ? null : new String[doubleFieldCount];
            this.booleanFields = booleanFieldCount == 0 ? null : new Field[booleanFieldCount];
            this.booleanFieldNames = booleanFieldCount == 0 ? null : new String[booleanFieldCount];
            this.otherFields = otherFieldCount == 0 ? null : new Field[otherFieldCount];
            this.otherFieldNames = otherFieldCount == 0 ? null : new String[otherFieldCount];
            initFieldProbes(level, probedFields);
        }

        private void initMethodProbes(ProbeLevel level, List<Method> probes) {
            int i = 0;
            for (Method m : probes) {
                Probe p = m.getAnnotation(Probe.class);
                if (p == null || p.level() == level) {
                    String name = p == null ? "" : p.name();
                    if (name.isEmpty()) {
                        name = getterIntoProperty(m.getName());
                    }
                    methods[i] = m;
                    methodNames[i++] = name;
                }
            }
        }

        private void initFieldProbes(ProbeLevel level, List<Field> probes) {
            int longIndex = 0;
            int doubleIndex = 0;
            int booleanIndex = 0;
            int otherIndex = 0;
            for (Field f : probes) {
                Probe p = f.getAnnotation(Probe.class);
                if (p.level() == level) {
                    String name = p.name();
                    if (name.isEmpty()) {
                        name = f.getName();
                    }
                    Class<?> valueType = f.getType();
                    if (valueType.isPrimitive()) {
                        if (valueType == double.class || valueType == float.class) {
                            doubleFields[doubleIndex] = f;
                            doubleFieldNames[doubleIndex++] = name;
                        } else if (valueType == boolean.class) {
                            booleanFields[booleanIndex] = f;
                            booleanFieldNames[booleanIndex++] = name;
                        } else {
                            longFields[longIndex] = f;
                            longFieldNames[longIndex++] = name;
                        }
                    } else {
                        otherFields[otherIndex] = f;
                        otherFieldNames[otherIndex++] = name;
                    }
                }
            }
        }

        private static int countMethodProbesWith(ProbeLevel level, List<Method> probes) {
            int c = 0;
            for (Method m : probes) {
                Probe probe = m.getAnnotation(Probe.class);
                if (probe == null || probe.level() == level) {
                    c++;
                }
            }
            return c;
        }

        private static int countFieldProbesWith(ProbeLevel level, List<Field> probes, 
                Class<?>... filtered) {
            int c = 0;
            for (Field f : probes) {
                if (f.getAnnotation(Probe.class).level() == level && contains(filtered, f.getType())) {
                    c++;
                }
            }
            return c;
        }

        private static boolean contains(Class<?>[] filtered, Class<?> type) {
            if (filtered.length == 0) {
                return true;
            }
            for (Class<?> t : filtered) {
                if (t == type) {
                    return true;
                }
            }
            return false;
        }

        void probeIn(ProbingCycle cycle, Object instance) {
            probeMethods(cycle, instance);
            probeLongFields(cycle, instance);
            probeDoubleFields(cycle, instance);
            probeBooleanFields(cycle, instance);
            probeOtherFields(cycle, instance);
        }

        private void probeOtherFields(ProbingCycle cycle, Object instance) {
            if (otherFields != null) {
                for (int i = 0; i < otherFields.length; i++) {
                    try {
                        cycle.probe(level, otherFieldNames[i], toLong(otherFields[i].get(instance)));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read field probe", e);
                    }
                }
            }
        }

        private void probeBooleanFields(ProbingCycle cycle, Object instance) {
            if (booleanFields != null) {
                for (int i = 0; i < booleanFields.length; i++) {
                    try {
                        cycle.probe(level, booleanFieldNames[i], booleanFields[i].getBoolean(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read boolean field probe", e);
                    }
                }
            }
        }

        private void probeDoubleFields(ProbingCycle cycle, Object instance) {
            if (doubleFields != null) {
                for (int i = 0; i < doubleFields.length; i++) {
                    try {
                        cycle.probe(level, doubleFieldNames[i], doubleFields[i].getDouble(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read double field probe", e);
                    }
                }
            }
        }

        private void probeLongFields(ProbingCycle cycle, Object instance) {
            if (longFields != null) {
                for (int i = 0; i < longFields.length; i++) {
                    try {
                        cycle.probe(level, longFieldNames[i], longFields[i].getLong(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read long field probe", e);
                    }
                }
            }
        }

        private void probeMethods(ProbingCycle cycle, Object instance) {
            if (methods != null) {
                for (int i = 0; i < methods.length; i++) {
                    try {
                        cycle.probe(level, methodNames[i], 
                                toLong(methods[i].invoke(instance, EMPTY_ARGS)));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read method probe", e);
                    }
                }
            }
        }
    }
    
    private static final class ProbeAnnotatedType {

        private final String prefix;
        private final ProbeAnnotatedTypeLevel[] levels = 
                new ProbeAnnotatedTypeLevel[ProbeLevel.values().length];

        ProbeAnnotatedType(Class<?> type) {
            prefix = type.isAnnotationPresent(Probe.class) ? type.getAnnotation(Probe.class).name()
                    : null;
            initByAnnotations(type);
        }

        ProbeAnnotatedType(Class<?> type, ProbeLevel level, String... methodNames) {
            prefix = null;
            initByNameList(type, level, methodNames);
        }

        private void initByNameList(Class<?> type, ProbeLevel level, String... methodNames) {
            List<Method> probedMethods = new ArrayList<Method>();
            for (String name : methodNames) {
                try {
                    Method m = type.getDeclaredMethod(name);
                    m.setAccessible(true);
                    probedMethods.add(m);
                } catch (Exception e) {
                    LOGGER.warning("Expected probe method `" + name + "` does not exist for type: "
                            + type.getName());
                }
            }
            levels[level.ordinal()] = ProbeAnnotatedTypeLevel.createIfNeeded(level, probedMethods,
                    Collections.<Field>emptyList());
        }

        private void initByAnnotations(Class<?> type) {
            List<Method> probedMethods = new ArrayList<Method>();
            collectProbeMethods(type, probedMethods);
            List<Field> probedFields = new ArrayList<Field>();
            collectProbeFields(type, probedFields);
            if (!probedMethods.isEmpty() || !probedFields.isEmpty()) {
                for (ProbeLevel level : ProbeLevel.values()) {
                    levels[level.ordinal()] = ProbeAnnotatedTypeLevel.createIfNeeded(level,
                            probedMethods, probedFields);
                }
            }
        }

        void probeIn(ProbingCycleImpl cycle, Object instance, ProbeLevel level) {
            if (prefix != null) {
                cycle.prefix(prefix);
            }
            for (int i = 0; i < levels.length; i++) {
                ProbeAnnotatedTypeLevel l = levels[i];
                if (l != null && l.level.isEnabled(level)) {
                    l.probeIn(cycle, instance);
                }
            }
        }

        private static void collectProbeMethods(Class<?> type, List<Method> probes) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(Probe.class)) {
                    if (!type.isInterface() || !hasMethod(m, probes)) {
                        m.setAccessible(true);
                        probes.add(m);
                    }
                }
            }
            if (type.getSuperclass() != null) {
                collectProbeMethods(type.getSuperclass(), probes);
            }
            for (Class<?> t : type.getInterfaces()) {
                collectProbeMethods(t, probes);
            }
        }

        private static boolean hasMethod(Method probe, List<Method> probes) {
            for (Method p : probes) {
                if (p.getName().equals(probe.getName()))
                    return true;
            }
            return false;
        }

        private static void collectProbeFields(Class<?> type, List<Field> probes) {
            for (Field f : type.getDeclaredFields()) {
                if (f.isAnnotationPresent(Probe.class)) {
                    f.setAccessible(true);
                    probes.add(f);
                }
            }
            if (type.getSuperclass() != null) {
                collectProbeFields(type.getSuperclass(), probes);
            }
        }
    }

    static void appendEscaped(StringBuilder buf, CharSequence value) {
        int len = value.length();
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (c == ',' || c == ' ' || c == '\\' || c == '=') {
                buf.append('\\');
            }
            buf.append(c);
        }
    }

    static long toLong(double value) {
        return round(value * 10000d);
    }

    static long toLong(Object value) {
        if (value == null) {
            return -1L;
        }
        Class<?> type = value.getClass();
        if (value instanceof Number) {
            if (type == Float.class || type == Double.class) {
                return toLong(((Number) value).doubleValue());
            }
            return ((Number) value).longValue();
        }
        if (type == Boolean.class) {
            return ((Boolean) value).booleanValue() ? 1 : 0;
        }
        if (type == AtomicBoolean.class) {
            return ((AtomicBoolean) value).get() ? 1 : 0;
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
