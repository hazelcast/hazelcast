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

package com.hazelcast.internal.metrics.impl;

import static com.hazelcast.internal.metrics.CharSequenceUtils.appendEscaped;
import static com.hazelcast.internal.metrics.CharSequenceUtils.appendUnescaped;
import static com.hazelcast.internal.metrics.ProbeUtils.findProbedFields;
import static com.hazelcast.internal.metrics.ProbeUtils.findProbedMethods;
import static com.hazelcast.internal.metrics.ProbeUtils.isSuitableProbeMethod;
import static com.hazelcast.internal.metrics.ProbeUtils.isSupportedProbeType;
import static com.hazelcast.internal.metrics.ProbeUtils.probeName;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.internal.metrics.BeforeCollectionCycle;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.CollectionContext;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.ProbeUtils;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.ProbingContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class MetricsRegistryImpl implements MetricsRegistry {

    private static final ILogger LOGGER = Logger.getLogger(MetricsRegistryImpl.class);

    private static final Object[] EMPTY_ARGS = new Object[0];

    private final ConcurrentMap<Class<?>, ProbeAnnotatedType> metaDataCache =
            new ConcurrentReferenceHashMap<Class<?>, ProbeAnnotatedType>();

    private final ConcurrentMap<Class<?>, MetricsSourceEntry> sources =
            new ConcurrentHashMap<Class<?>, MetricsRegistryImpl.MetricsSourceEntry>();


    static ProbeAnnotatedType register(ConcurrentMap<Class<?>, ProbeAnnotatedType> cache,
            Class<?> type, ProbeAnnotatedType metadata) {
        ProbeAnnotatedType existing = cache.putIfAbsent(type, metadata);
        return existing == null ? metadata : existing;
    }

    static ProbeAnnotatedType getOrCreate(ConcurrentMap<Class<?>, ProbeAnnotatedType> cache,
            Class<?> type) {
        ProbeAnnotatedType metadata = cache.get(type);
        // main goal was to avoid creating expensive metadata but at this point we have to
        return metadata != null ? metadata : register(cache, type, new ProbeAnnotatedType(type));
    }

    private void registerSource(MetricsSource source) {
        if (!sources.containsKey(source.getClass())) {
            sources.putIfAbsent(source.getClass(), new MetricsSourceEntry(source));
        } else {
            LOGGER.info(MetricsSource.class.getSimpleName() + " tried to register more then once: "
                    + source.getClass().getSimpleName());
        }
    }

    @Override
    public void register(final Object source) {
        if (source instanceof MetricsSource) {
            registerSource((MetricsSource) source);
        } else if (source != null) {
            if (getOrCreate(metaDataCache, source.getClass()).hasProbes()) {
                registerSource(new MetricsSource() {
                    @Override
                    public void collectAll(CollectionCycle cycle) {
                        cycle.probe(source);
                    }
                });
            }
        }
    }

    @Override
    public CollectionContext openContext(Class<? extends MetricsSource>... selection) {
        return new CollectionCycleImpl(metaDataCache, sources.values(), selection);
    }

    /**
     * Wrapper for a {@link MetricsSource} for the state to keep track of it potential
     * need to be updated before being probed.
     */
    private static final class MetricsSourceEntry {

        final MetricsSource source;
        final Method update;
        final long updateIntervalMs;
        final AtomicLong nextUpdateTimeMs = new AtomicLong();
        final ProbeLevel updateLevel;

        public MetricsSourceEntry(MetricsSource source) {
            this.source = source;
            this.update = reprobeFor(source.getClass());
            if (update != null) {
                BeforeCollectionCycle reprobe = update.getAnnotation(BeforeCollectionCycle.class);
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
                if (m.isAnnotationPresent(BeforeCollectionCycle.class)) {
                    m.setAccessible(true);
                    return m;
                }
            }
            return null;
        }
    }

    /**
     * A {@link CollectionContext} and the {@link CollectionCycle} it represents as
     * well are thread specific (per thread) and therefore not thread-safe or build
     * to be used by or from multiple threads.
     */
    private static final class CollectionCycleImpl
    implements CollectionCycle, CollectionCycle.Tags, CollectionContext {

        // shared state
        private final ConcurrentMap<Class<?>, ProbeAnnotatedType> mataDataCache;

        // collection context state
        private final Collection<MetricsSourceEntry> sources;
        private final Class<? extends MetricsSource>[] selection;
        private final List<MetricsSourceEntry> effectiveSource = new ArrayList<MetricsSourceEntry>();
        private int sourceCount;

        // collection cycle state
        private final StringBuilder tags = new StringBuilder(128);
        private int tagBaseIndex;
        private MetricsCollector collector;
        private ProbeLevel level;
        private CharSequence lastTagName;
        private int lastTagValuePosition;

        CollectionCycleImpl(ConcurrentMap<Class<?>, ProbeAnnotatedType> mataDataCache,
                Collection<MetricsSourceEntry> sources, Class<? extends MetricsSource>[] selection) {
            this.mataDataCache = mataDataCache;
            this.sources = sources;
            this.selection = selection;
        }

        private void updateSources() {
            int size = sources.size();
            if (sourceCount != size) {
                sourceCount = size;
                effectiveSource.clear();
                for (MetricsSourceEntry e : sources) {
                    if (activeSource(e.source.getClass())) {
                        effectiveSource.add(e);
                    }
                }
            }
        }

        private boolean activeSource(Class<?> source) {
            if (selection == null || selection.length == 0) {
                return true;
            }
            for (Class<?> accepted : selection) {
                if (accepted == source) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void collectAll(ProbeLevel level, MetricsCollector collector) {
            updateSources();
            this.level = level;
            this.collector = collector;
            for (MetricsSourceEntry entry : effectiveSource) {
                if (isProbed(entry.updateLevel)) {
                    entry.updateIfNeeded();
                }
                try {
                    tagBaseIndex = 0;
                    openContext();
                    entry.source.collectAll(this);
                } catch (Exception e) {
                    LOGGER.warning("Exception while collecting source "
                            + entry.source.getClass().getSimpleName(), e);
                }
            }
        }

        @Override
        public void collect(ProbeLevel level, Object instance, String[] methods) {
            if (instance == null || !isProbed(level)) {
                return;
            }
            Class<?> type = instance.getClass();
            ProbeAnnotatedType metadata = mataDataCache.get(type);
            if (metadata == null) {
                // main goal was to avoid creating expensive metadata but at this point we have to
                metadata = register(mataDataCache, type, new ProbeAnnotatedType(type, level, methods));
            }
            metadata.collectAll(this, this.level, null, instance);
        }

        @Override
        public void probe(Object instance) {
            probe(null, instance);
        }

        @Override
        public void probe(CharSequence prefix, Object instance) {
            if (instance != null) {
                getOrCreate(mataDataCache, instance.getClass()).collectAll(this, level, prefix, instance);
            }
        }

        @Override
        public void collectAll(CharSequence prefix, MetricsSource source) {
            if (source == null || !activeSource(source.getClass())) {
                return;
            }
            int baseIndex = tagBaseIndex;
            CharSequence lastTag = lastTagName;
            prefix(prefix);
            tagBaseIndex = tags.length();
            source.collectAll(this);
            tagBaseIndex = baseIndex;
            lastTagName = lastTag;
            tags.setLength(tagBaseIndex);
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
        public void collect(CharSequence name, long value) {
            collect(ProbeLevel.INFO, name, value);
        }

        @Override
        public void collect(CharSequence name, double value) {
            collect(ProbeLevel.INFO, name, value);
        }

        @Override
        public void collect(CharSequence name, boolean value) {
            collect(ProbeLevel.INFO, name, value);
        }

        @Override
        public void collect(ProbeLevel level, CharSequence name, long value) {
            if (isProbed(level)) {
                int len0 = tags.length();
                appendEscaped(tags, name);
                collector.collect(tags, value);
                tags.setLength(len0);
            }
        }

        @Override
        public void collect(ProbeLevel level, CharSequence name, double value) {
            collect(level, name, ProbeUtils.toLong(value));
        }

        @Override
        public void collect(ProbeLevel level, CharSequence name, boolean value) {
            collect(level, name, value ? 1 : 0);
        }

        @Override
        public void collectForwarded(CharSequence name, long value) {
            int len0 = tags.length();
            appendUnescaped(tags, name);
            collector.collect(tags, value);
            tags.setLength(len0);
        }

        @Override
        public Tags openContext() {
            tags.setLength(tagBaseIndex);
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
            if (prefix.length() > 0 && !Probe.BLANK_NAME.contentEquals(prefix)) {
                return append(prefix).append(".");
            }
            return this;
        }

        @Override
        public Tags adjoin(CharSequence s) {
            if (s.length() > 0) {
                int lastIndex = tags.length() - 1;
                if (tags.charAt(lastIndex) != ' ') {
                    return append(s);
                }
                tags.setLength(lastIndex);
                appendEscaped(tags, s);
                tags.append(' ');
            }
            return this;
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
        final boolean[] otherFieldNested;

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
            this.otherFieldNested = otherFieldCount == 0 ? null : new boolean[otherFieldCount];
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
                    methods[i] = m;
                    methodNames[i++] = probeName(p, m);
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
                    String name = probeName(p, f);
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
                        boolean nested = MetricsSource.class.isAssignableFrom(valueType)
                                        || valueType.isAnnotationPresent(Probe.class);
                        otherFieldNested[otherIndex] = nested;
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

        void probeIn(CollectionCycle cycle, Object instance) {
            probeMethods(cycle, instance);
            probeLongFields(cycle, instance);
            probeDoubleFields(cycle, instance);
            probeBooleanFields(cycle, instance);
            probeOtherFields(cycle, instance);
        }

        private void probeOtherFields(CollectionCycle cycle, Object instance) {
            if (otherFields != null) {
                for (int i = 0; i < otherFields.length; i++) {
                    try {
                        Object value = otherFields[i].get(instance);
                        String name = otherFieldNames[i];
                        if (otherFieldNested[i]) {
                            if (value instanceof MetricsSource) {
                                cycle.collectAll(name, (MetricsSource) value);
                            } else {
                                cycle.probe(name, value);
                            }
                        } else {
                            cycle.collect(level, name, ProbeUtils.toLong(value));
                        }
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read field probe", e);
                    }
                }
            }
        }

        private void probeBooleanFields(CollectionCycle cycle, Object instance) {
            if (booleanFields != null) {
                for (int i = 0; i < booleanFields.length; i++) {
                    try {
                        cycle.collect(level, booleanFieldNames[i], booleanFields[i].getBoolean(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read boolean field probe", e);
                    }
                }
            }
        }

        private void probeDoubleFields(CollectionCycle cycle, Object instance) {
            if (doubleFields != null) {
                for (int i = 0; i < doubleFields.length; i++) {
                    try {
                        cycle.collect(level, doubleFieldNames[i], doubleFields[i].getDouble(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read double field probe", e);
                    }
                }
            }
        }

        private void probeLongFields(CollectionCycle cycle, Object instance) {
            if (longFields != null) {
                for (int i = 0; i < longFields.length; i++) {
                    try {
                        cycle.collect(level, longFieldNames[i], longFields[i].getLong(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read long field probe", e);
                    }
                }
            }
        }

        private void probeMethods(CollectionCycle cycle, Object instance) {
            if (methods != null) {
                for (int i = 0; i < methods.length; i++) {
                    try {
                        cycle.collect(level, methodNames[i],
                                ProbeUtils.toLong(methods[i].invoke(instance, EMPTY_ARGS)));
                    } catch (InvocationTargetException e) {
                        LOGGER.warning(
                                "@Probe method `" + methods[i].getName() + "` throw exception:",
                                e.getTargetException());
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read method probe: " + methods[i].getName(), e);
                    }
                }
            }
        }
    }

    /**
     * Meta-data for a type that {@link Probe} annotated fields or methods.
     */
    private static final class ProbeAnnotatedType {

        private final boolean addsContext;
        private final String fixedPrefix;
        private final ProbeAnnotatedTypeLevel[] levels =
                new ProbeAnnotatedTypeLevel[ProbeLevel.values().length];

        ProbeAnnotatedType(Class<?> type) {
            addsContext = ProbingContext.class.isAssignableFrom(type);
            fixedPrefix = type.isAnnotationPresent(Probe.class) ? type.getAnnotation(Probe.class).name()
                    : null;
            initByAnnotations(type);
        }

        ProbeAnnotatedType(Class<?> type, ProbeLevel level, String... methodNames) {
            addsContext = ProbingContext.class.isAssignableFrom(type);
            fixedPrefix = null;
            initByNameList(type, level, methodNames);
        }

        boolean hasProbes() {
            for (int i = 0; i < levels.length; i++) {
                if (levels[i] != null) {
                    return true;
                }
            }
            return false;
        }

        private void initByNameList(Class<?> type, ProbeLevel level, String... methodNames) {
            List<Method> probedMethods = new ArrayList<Method>();
            for (String name : methodNames) {
                try {
                    Method m = type.getDeclaredMethod(name);
                    if (isSuitableProbeMethod(m, probedMethods) && isSupportedProbeType(m.getReturnType())) {
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
            List<Field> probedFields = findProbedFields(type);
            List<Method> probedMethods = findProbedMethods(type);
            removeMethodProbesOverridenByFieldProbes(probedFields, probedMethods);
            if (!probedMethods.isEmpty() || !probedFields.isEmpty()) {
                for (ProbeLevel level : ProbeLevel.values()) {
                    levels[level.ordinal()] = ProbeAnnotatedTypeLevel.createIfNeeded(level,
                            probedMethods, probedFields);
                }
            }
        }

        private void removeMethodProbesOverridenByFieldProbes(List<Field> probedFields, List<Method> probedMethods) {
            if (!probedMethods.isEmpty() && !probedFields.isEmpty()) {
                Set<String> fieldProbeNames = new HashSet<String>();
                for (Field f : probedFields) {
                    fieldProbeNames.add(probeName(f.getAnnotation(Probe.class), f));
                }
                Iterator<Method> iter = probedMethods.iterator();
                while (iter.hasNext()) {
                    Method m = iter.next();
                    if (fieldProbeNames.contains(probeName(m.getAnnotation(Probe.class), m))) {
                        iter.remove();
                    }
                }
            }
        }

        void collectAll(CollectionCycleImpl cycle, ProbeLevel level, CharSequence dynamicPrefix,
                Object instance) {
            if (addsContext) {
                collectAllInContext(cycle, level, dynamicPrefix, instance);
            } else {
                collectAllInternal(cycle, level, dynamicPrefix, instance);
            }
        }

        /**
         * When instance itself is adding tags the tag context has to be restored as
         * well.
         */
        private void collectAllInContext(CollectionCycleImpl cycle, ProbeLevel level,
                CharSequence dynamicPrefix, Object instance) {
            CharSequence lastTagName = cycle.lastTagName;
            int lastTagValuePosition = cycle.lastTagValuePosition;
            collectAllInternal(cycle, level, dynamicPrefix, instance);
            cycle.lastTagName = lastTagName;
            cycle.lastTagValuePosition = lastTagValuePosition;
        }

        private void collectAllInternal(CollectionCycleImpl cycle, ProbeLevel level,
                CharSequence dynamicPrefix, Object instance) {
            int len0 = cycle.tags.length();
            if (addsContext) {
                ((ProbingContext) instance).tag(cycle);
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
