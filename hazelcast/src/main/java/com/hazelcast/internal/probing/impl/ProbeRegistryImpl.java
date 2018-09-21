/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.probing.impl;

import static com.hazelcast.internal.probing.CharSequenceUtils.appendEscaped;
import static com.hazelcast.internal.probing.CharSequenceUtils.appendUnescaped;
import static com.hazelcast.util.StringUtil.getterIntoProperty;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeRenderContext;
import com.hazelcast.internal.probing.ProbeRenderer;
import com.hazelcast.internal.probing.ProbeSource;
import com.hazelcast.internal.probing.ProbeUtils;
import com.hazelcast.internal.probing.ProbingCycle;
import com.hazelcast.internal.probing.Tagging;
import com.hazelcast.internal.probing.BeforeProbeCycle;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class ProbeRegistryImpl implements ProbeRegistry {

    private static final ILogger LOGGER = Logger.getLogger(ProbeRegistryImpl.class);

    private static final Object[] EMPTY_ARGS = new Object[0];

    private static final ConcurrentMap<Class<?>, ProbeAnnotatedType> PROBE_METADATA =
            new ConcurrentHashMap<Class<?>, ProbeAnnotatedType>();

    private final Queue<ProbeSourceEntry> sources = new ConcurrentLinkedQueue<ProbeSourceEntry>();

    private static ProbeAnnotatedType register(Class<?> type, ProbeAnnotatedType metadata) {
        ProbeAnnotatedType existing = PROBE_METADATA.putIfAbsent(type, metadata);
        return existing == null ? metadata : existing;
    }

    @Override
    public void register(ProbeSource source) {
        for (ProbeSourceEntry e : sources) {
            if (e.source.equals(source)) {
                LOGGER.info("Probe source tried to register more then once: "
                        + source.getClass().getSimpleName());
                // avoid adding the very same instance more then once
                return;
            }
        }
        sources.add(new ProbeSourceEntry(source));
    }

    @Override
    public void registerIfSource(Object source) {
        if (source instanceof ProbeSource) {
            register((ProbeSource) source);
        }
    }

    @Override
    public ProbeRenderContext newRenderContext() {
        return new ProbingCycleImpl(sources);
    }

    @Override
    public ProbeRenderContext newRenderContext(Class<? extends ProbeSource>... filter) {
        Set<ProbeSourceEntry> filtered = new HashSet<ProbeSourceEntry>();
        for (ProbeSourceEntry e : sources) {
            for (Class<?> accepted : filter) {
                if (accepted == e.source.getClass()) {
                    filtered.add(e);
                }
            }
        }
        return new ProbingCycleImpl(filtered);
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
        final ProbeLevel updateLevel;

        public ProbeSourceEntry(ProbeSource source) {
            this.source = source;
            this.update = reprobeFor(source.getClass());
            if (update != null) {
                BeforeProbeCycle reprobe = update.getAnnotation(BeforeProbeCycle.class);
                updateIntervalMs = ProbeUtils.updateInterval(reprobe.value(), reprobe.unit());
                updateLevel = reprobe.level();
            } else {
                updateIntervalMs = -1L;
                updateLevel = ProbeLevel.DEBUG;
            }
        }

        void updateIfNeeded() {
            if (update != null) {
                long next = nextUpdateTimeMs.get();
                long now = Clock.currentTimeMillis();
                if (now > next && nextUpdateTimeMs.compareAndSet(next, now + updateIntervalMs)) {
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
                if (m.isAnnotationPresent(BeforeProbeCycle.class)) {
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
        private final Collection<ProbeSourceEntry> sources;
        private CharSequence lastTagName;
        private int lastTagValuePosition;

        // render cycle state
        private ProbeRenderer renderer;
        private ProbeLevel level;

        ProbingCycleImpl(Collection<ProbeSourceEntry> sources) {
            this.sources = sources;
        }

        @Override
        public void render(ProbeLevel level, ProbeRenderer renderer) {
            this.level = level;
            this.renderer = renderer;
            for (ProbeSourceEntry entry : sources) {
                if (isProbed(entry.updateLevel)) {
                    entry.updateIfNeeded();
                }
                try {
                    openContext();
                    entry.source.probeIn(this);
                } catch (Exception e) {
                    LOGGER.warning("Exception while probing source "
                            + entry.source.getClass().getSimpleName(), e);
                }
            }
        }

        @Override
        public void probe(ProbeLevel level, Object instance, String[] methods) {
            if (instance == null || !isProbed(level)) {
                return;
            }
            Class<?> type = instance.getClass();
            ProbeAnnotatedType metadata = PROBE_METADATA.get(type);
            if (metadata == null) {
                // main goal was to avoid creating expensive metadata but at this point we have to
                metadata = register(type, new ProbeAnnotatedType(type, level, methods));
            }
            metadata.probeIn(this, this.level, null, instance);
        }

        @Override
        public void probe(Object instance) {
            probe(null, instance);
        }

        @Override
        public void probe(CharSequence prefix, Object instance) {
            if (instance == null) {
                return;
            }
            Class<?> type = instance.getClass();
            ProbeAnnotatedType metadata = PROBE_METADATA.get(type);
            if (metadata == null) {
                // main goal was to avoid creating expensive metadata but at this point we have to
                metadata = register(type, new ProbeAnnotatedType(type));
            }
            metadata.probeIn(this, level, prefix, instance);
        }

        @Override
        public void probe(Object[] instances) {
            if (instances != null) {
                for (int i = 0; i < instances.length; i++) {
                    probe(instances[i]);
                }
            }
        }

        @Override
        public void probe(CharSequence name, long value) {
            probe(ProbeLevel.INFO, name, value);
        }

        @Override
        public void probe(CharSequence name, double value) {
            probe(ProbeLevel.INFO, name, value);
        }

        @Override
        public void probe(CharSequence name, boolean value) {
            probe(ProbeLevel.INFO, name, value);
        }

        @Override
        public void probe(ProbeLevel level, CharSequence name, long value) {
            if (isProbed(level)) {
                render(name, value);
            }
        }

        @Override
        public void probe(ProbeLevel level, CharSequence name, double value) {
            probe(level, name, ProbeUtils.toLong(value));
        }

        @Override
        public void probe(ProbeLevel level, CharSequence name, boolean value) {
            probe(level, name, value ? 1 : 0);
        }

        @Override
        public void probeForwarded(CharSequence name, long value) {
            int len0 = tags.length();
            appendUnescaped(tags, name);
            renderer.render(tags, value);
            tags.setLength(len0);
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
                appendEscaped(tags, value);
                tags.append(' ');
                return this;
            }
            tags.append(name).append('=');
            lastTagName = name;
            lastTagValuePosition = tags.length();
            appendEscaped(tags, value);
            tags.append(' ');
            return this;
        }

        @Override
        public Tags tag(CharSequence name, long value) {
            if (name == lastTagName) {
                tags.setLength(lastTagValuePosition);
                tags.append(value);
                tags.append(' ');
                return this;
            }
            tags.append(name).append('=');
            lastTagName = name;
            lastTagValuePosition = tags.length();
            tags.append(value);
            tags.append(' ');
            return this;
        }

        @Override
        public Tags append(CharSequence s) {
            // s might contain user supplied values
            appendEscaped(tags, s);
            return this;
        }

        @Override
        public Tags prefix(CharSequence prefix) {
            if (prefix.length() > 0) {
                return append(prefix).append(".");
            }
            return this;
        }

        private void render(CharSequence name, long value) {
            int len0 = tags.length();
            appendEscaped(tags, name);
            renderer.render(tags, value);
            tags.setLength(len0);
        }

        @Override
        public boolean isProbed(ProbeLevel level) {
            return level.isEnabled(this.level);
        }

        @Override
        public String toString() {
            return tags.toString();
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

        @SuppressWarnings("checkstyle:npathcomplexity")
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
            sort(methodNames, methods);
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
            sort(longFieldNames, longFields);
            sort(doubleFieldNames, doubleFields);
            sort(booleanFieldNames, booleanFields);
            sort(otherFieldNames, otherFields);
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
                        cycle.probe(level, otherFieldNames[i], ProbeUtils.toLong(otherFields[i].get(instance)));
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
                                ProbeUtils.toLong(methods[i].invoke(instance, EMPTY_ARGS)));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read method probe: " + methods[i].getName(), e);
                    }
                }
            }
        }
    }

    private static final class ProbeAnnotatedType {

        private final boolean tagging;
        private final String fixedPrefix;
        private final ProbeAnnotatedTypeLevel[] levels =
                new ProbeAnnotatedTypeLevel[ProbeLevel.values().length];

        ProbeAnnotatedType(Class<?> type) {
            tagging = Tagging.class.isAssignableFrom(type);
            fixedPrefix = type.isAnnotationPresent(Probe.class) ? type.getAnnotation(Probe.class).name()
                    : null;
            initByAnnotations(type);
        }

        ProbeAnnotatedType(Class<?> type, ProbeLevel level, String... methodNames) {
            tagging = Tagging.class.isAssignableFrom(type);
            fixedPrefix = null;
            initByNameList(type, level, methodNames);
        }

        private void initByNameList(Class<?> type, ProbeLevel level, String... methodNames) {
            List<Method> probedMethods = new ArrayList<Method>();
            for (String name : methodNames) {
                try {
                    Method m = type.getDeclaredMethod(name);
                    if (isSuitableProbeMethod(m, probedMethods)) {
                        m.setAccessible(true);
                        probedMethods.add(m);
                    }
                } catch (NoSuchMethodException e) {
                    LOGGER.warning("Expected probe method `" + name + "` does not exist for type: "
                            + type.getName());
                } catch (Exception e) {
                    LOGGER.warning("Failed to add probe method `" + name + "`: " + e.getMessage());
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

        void probeIn(ProbingCycleImpl cycle, ProbeLevel level, CharSequence dynamicPrefix,
                Object instance) {
            if (tagging) {
                probeTagging(cycle, level, dynamicPrefix, instance);
            } else {
                probeNotTagging(cycle, level, dynamicPrefix, instance);
            }
        }

        /**
         * When instance itself is adding tags the tag context has to be restored as
         * well.
         */
        private void probeTagging(ProbingCycleImpl cycle, ProbeLevel level,
                CharSequence dynamicPrefix, Object instance) {
            CharSequence lastTagName = cycle.lastTagName;
            int lastTagValuePosition = cycle.lastTagValuePosition;
            probeNotTagging(cycle, level, dynamicPrefix, instance);
            cycle.lastTagName = lastTagName;
            cycle.lastTagValuePosition = lastTagValuePosition;
        }

        private void probeNotTagging(ProbingCycleImpl cycle, ProbeLevel level,
                CharSequence dynamicPrefix, Object instance) {
            int len0 = cycle.tags.length();
            if (tagging) {
                ((Tagging) instance).tagIn(cycle);
            }
            if (dynamicPrefix != null) {
                cycle.prefix(dynamicPrefix);
            }
            if (fixedPrefix != null) {
                cycle.prefix(fixedPrefix);
            }
            for (int i = 0; i < levels.length; i++) {
                ProbeAnnotatedTypeLevel l = levels[i];
                if (l != null && l.level.isEnabled(level)) {
                    l.probeIn(cycle, instance);
                }
            }
            cycle.tags.setLength(len0);
        }

        private static void collectProbeMethods(Class<?> type, List<Method> probes) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(Probe.class) && isSuitableProbeMethod(m, probes)) {
                    m.setAccessible(true);
                    probes.add(m);
                }
            }
            if (type.getSuperclass() != null) {
                collectProbeMethods(type.getSuperclass(), probes);
            }
            for (Class<?> t : type.getInterfaces()) {
                collectProbeMethods(t, probes);
            }
        }

        private static boolean isSuitableProbeMethod(Method m, List<Method> probes) {
            if (m.getReturnType() == void.class) {
                LOGGER.warning("Probe method must return something: " + m.toGenericString());
                return false;
            }
            if (m.getParameterTypes().length > 0) {
                LOGGER.warning("Probe method must not have parameters: " + m.toGenericString());
                return false;
            }
            return !m.getDeclaringClass().isInterface() || !hasMethod(m, probes);
        }

        private static boolean hasMethod(Method probe, List<Method> probes) {
            for (Method p : probes) {
                if (p.getName().equals(probe.getName())) {
                    return true;
                }
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

    static <T> void sort(String[] names, T[] values) {
        if (names == null) {
            return;
        }
        String[] unsortedNames = names.clone();
        T[] unsortedValues = values.clone();
        Arrays.sort(names);
        for (int i = 0; i < names.length; i++) {
            values[i] = unsortedValues[indexOf(unsortedNames, names[i])];
        }
    }

    @SuppressFBWarnings(value = "ES_COMPARING_PARAMETER_STRING_WITH_EQ",
            justification = "== is intentionally used to find identical instance")
    private static int indexOf(String[] arr, String e) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == e) {
                return i;
            }
        }
        return -1;
    }
}
