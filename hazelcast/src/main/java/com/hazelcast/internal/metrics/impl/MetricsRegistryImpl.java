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

import static com.hazelcast.internal.metrics.CharSequenceUtils.appendUnescaped;
import static com.hazelcast.internal.metrics.MetricsSource.TAG_INSTANCE;
import static com.hazelcast.internal.metrics.MetricsSource.TAG_NAMESPACE;
import static com.hazelcast.internal.metrics.ProbeUtils.findProbedFields;
import static com.hazelcast.internal.metrics.ProbeUtils.findProbedMethods;
import static com.hazelcast.internal.metrics.ProbeUtils.isSuitableProbeMethod;
import static com.hazelcast.internal.metrics.ProbeUtils.isSupportedProbeType;
import static com.hazelcast.internal.metrics.ProbeUtils.probeName;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
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
import com.hazelcast.internal.metrics.CharSequenceUtils;
import com.hazelcast.internal.metrics.CollectionContext;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.ObjectMetricsContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeSource;
import com.hazelcast.internal.metrics.ProbeUtils;
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

    private final ConcurrentMap<Integer, Object> roots =
            new ConcurrentHashMap<Integer, Object>();

    private final ConcurrentMap<String, LongGauge> longGauges =
            new ConcurrentHashMap<String, LongGauge>();

    private final ConcurrentMap<String, DoubleGauge> doubleGauges =
            new ConcurrentHashMap<String, DoubleGauge>();

    static ProbeAnnotatedType register(ConcurrentMap<Class<?>, ProbeAnnotatedType> cache, ProbeAnnotatedType typeMetaData) {
        ProbeAnnotatedType existing = cache.putIfAbsent(typeMetaData.type, typeMetaData);
        return existing == null ? typeMetaData : existing;
    }

    static ProbeAnnotatedType getOrCreate(ConcurrentMap<Class<?>, ProbeAnnotatedType> cache, Class<?> type) {
        ProbeAnnotatedType typeMetaData = cache.get(type);
        // main goal was to avoid creating expensive metadata but at this point we have to
        return typeMetaData != null ? typeMetaData : register(cache, new ProbeAnnotatedType(type));
    }

    @Override
    public LongGauge newLongGauge(String name) {
        LongGauge gauge = longGauges.get(name);
        return gauge != null ? gauge : longGauges.put(name, Gauges.longGauge());
    }

    @Override
    public DoubleGauge newDoubleGauge(String name) {
        DoubleGauge gauge = doubleGauges.get(name);
        return gauge != null ? gauge : doubleGauges.put(name, Gauges.doubleGauge());
    }

    @Override
    public void register(final Object root) {
        if (root == null) {
            return;
        }
        Class<?> type = root.getClass();
        ProbeAnnotatedType typeMetaData = getOrCreate(metaDataCache, type);
        if (!typeMetaData.isContributing) {
            LOGGER.fine("Ignored registration of " + type.getSimpleName() + " as it has no metrics.");
        } else if (roots.putIfAbsent(System.identityHashCode(root), root) != null) {
            LOGGER.info("Tried to register same root more then once: " + type.getSimpleName());
        }
    }

    @Override
    public CollectionContext openContext(ProbeLevel level) {
        return new CollectionCycleImpl(level, metaDataCache, longGauges, doubleGauges, roots.values());
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
        private final ConcurrentMap<String, LongGauge> longGauges;
        private final ConcurrentMap<String, DoubleGauge> doubleGauges;

        // collection context state
        private final Collection<Object> roots;
        private final ProbeLevel level;

        // collection cycle state
        private final StringBuilder tags = new StringBuilder(128);
        private int tagBaseIndex;
        private MetricsCollector collector;
        private CharSequence lastTagName;
        private int lastTagValuePosition;
        private boolean unconnectedGauges;
        private Object source;
        private Object gauge;

        CollectionCycleImpl(ProbeLevel level, ConcurrentMap<Class<?>, ProbeAnnotatedType> mataDataCache,
                ConcurrentMap<String, LongGauge> longGauges, ConcurrentMap<String, DoubleGauge> doubleGauges,
                Collection<Object> roots) {
            this.level = level;
            this.mataDataCache = mataDataCache;
            this.longGauges = longGauges;
            this.doubleGauges = doubleGauges;
            this.roots = roots;
        }

        @Override
        public void collectAll(MetricsCollector collector) {
            this.collector = collector;
            this.unconnectedGauges = !longGauges.isEmpty() || !doubleGauges.isEmpty();
            for (Object root : roots) {
                tagBaseIndex = 0;
                switchContext();
                collectAll(root);
            }
        }

        @Override
        public void collect(ProbeLevel level, Object instance, String[] methods) {
            if (instance == null || !isCollected(level)) {
                return;
            }
            Class<?> type = instance.getClass();
            ProbeAnnotatedType typeMetaData = mataDataCache.get(type);
            if (typeMetaData == null) {
                // main goal was to avoid creating expensive metadata but at this point we have to
                typeMetaData = register(mataDataCache, new ProbeAnnotatedType(type, level, methods));
            }
            typeMetaData.collectAll(this, instance);
        }

        @Override
        public void collectAll(Object obj) {
            if (obj instanceof Object[]) {
                collectAllOfArray((Object[]) obj);
            } else {
                collectAllOfObject(obj);
            }
        }

        private void collectAllOfObject(Object obj) {
            if (obj != null) {
                source = obj;
                getOrCreate(mataDataCache, obj.getClass()).collectAll(this, obj);
                source = null;
            }
        }

        private void collectAllOfArray(Object[] array) {
            if (array != null) {
                for (int i = 0; i < array.length; i++) {
                    collectAll(array[i]);
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
            if (isCollected(level)) {
                int len0 = tags.length();
                CharSequenceUtils.appendEscaped(tags, name);
                collector.collect(tags, value);
                if (unconnectedGauges) {
                    connectGauge();
                }
                tags.setLength(len0);
            }
        }

        private void connectGauge() {
            try {
                String key = tags.toString();
                if (longGauges.containsKey(key)) {
                    Gauges.connect(longGauges.remove(key), Gauges.longGauge(gauge, source));
                } else if (doubleGauges.containsKey(key)) {
                    Gauges.connect(doubleGauges.remove(key), Gauges.doubleGauge(gauge, source));
                }
            } catch (RuntimeException e) {
                LOGGER.warning("Failed to connect gauge: ", e);
            }
        }

        @Override
        public void collect(ProbeLevel level, CharSequence name, double value) {
            collect(level, name, ProbeUtils.toLong(value));
        }

        @Override
        public void collect(ProbeLevel level, CharSequence name, LongProbeFunction value) {
            gauge = value;
            collect(level, name, value.getAsLong());
            gauge = null;
        }

        @Override
        public void collect(ProbeLevel level, CharSequence name, DoubleProbeFunction value) {
            gauge = value;
            collect(level, name, value.getAsDouble());
            gauge = null;
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
        public Tags switchContext() {
            tags.setLength(tagBaseIndex);
            lastTagName = null;
            return this;
        }

        @Override
        public Tags tag(CharSequence name, CharSequence value) {
            return tag(name, value, null);
        }

        @Override
        public Tags tag(CharSequence name, CharSequence major, CharSequence minor) {
            if (name == lastTagName) {
                tags.setLength(lastTagValuePosition);
                appendEscaped(major, minor);
                tags.append(' ');
                return this;
            }
            tags.append(name).append('=');
            lastTagName = name;
            lastTagValuePosition = tags.length();
            appendEscaped(major, minor);
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
        public Tags namespace(CharSequence ns) {
            return tag(TAG_NAMESPACE, ns);
        }

        @Override
        public Tags namespace(CharSequence ns, CharSequence subSpace) {
            return tag(TAG_NAMESPACE, ns, subSpace);
        }

        @Override
        public Tags instance(CharSequence name) {
            return tag(TAG_INSTANCE, name);
        }

        @Override
        public Tags instance(CharSequence name, CharSequence subName) {
            return tag(TAG_INSTANCE, name, subName);
        }

        @Override
        public boolean isCollected(ProbeLevel level) {
            return level.isEnabled(this.level);
        }

        @Override
        public String toString() {
            return tags.toString();
        }

        private void appendEscaped(CharSequence major, CharSequence minor) {
            CharSequenceUtils.appendEscaped(tags, major);
            if (minor != null && minor.length() > 0) {
                tags.append('.');
                CharSequenceUtils.appendEscaped(tags, minor);
            }
        }
    }

    private static final class NamedList<T> {

        @SuppressWarnings("rawtypes")
        private static final NamedList EMPTY = new NamedList<Object>(new String[0], new Object[0]);

        final String[] names;
        final T[] values;
        private int size;

        private NamedList(String[] names, T[] values) {
            this.names = names;
            this.values = values;
        }

        @SuppressWarnings("unchecked")
        static <T> NamedList<T> of(Class<?> elementType, int size) {
            return size == 0 ? EMPTY : new NamedList<T>(new String[size], (T[]) Array.newInstance(elementType, size));
        }

        void add(String name, T value) {
            names[size] = name;
            values[size++] = value;
        }

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        void sort() {
            MetricsRegistryImpl.sort(names, values);
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
        final NamedList<Method> methods;
        final NamedList<Method> sourceMethods;
        final NamedList<Field> longFields;
        final NamedList<Field> doubleFields;
        final NamedList<Field> booleanFields;
        final NamedList<Field> otherFields;
        final NamedList<Field> sourceFields;

        ProbeAnnotatedTypeLevel(ProbeLevel level, List<Method> probedMethods, List<Field> probedFields) {
            this.level = level;
            this.methods = NamedList.of(Method.class, countMethodProbesWith(level, probedMethods));
            this.sourceMethods = NamedList.of(Method.class, countProbeSources(level, probedMethods));
            this.longFields = NamedList.of(Field.class, countFieldProbesWith(level, probedFields,
                    long.class, int.class, short.class, char.class, byte.class));
            this.doubleFields = NamedList.of(Field.class,
                    countFieldProbesWith(level, probedFields, double.class, float.class));
            this.booleanFields = NamedList.of(Field.class, countFieldProbesWith(level, probedFields, boolean.class));
            this.sourceFields = NamedList.of(Field.class, countProbeSources(level, probedFields));
            this.otherFields = NamedList.of(Field.class, countFieldProbesWith(level, probedFields));
            initAnnotatedFields(probedFields);
            initAnnotatedMethods(probedMethods);
            methods.sort();
            sourceMethods.sort();
            longFields.sort();
            doubleFields.sort();
            booleanFields.sort();
            otherFields.sort();
            sourceFields.sort();
        }

        @SuppressWarnings("checkstyle:booleanexpressioncomplexity")
        ProbeAnnotatedTypeLevel tidy() {
            return methods.isEmpty() && sourceMethods.isEmpty() && longFields.isEmpty()
                    && doubleFields.isEmpty() && booleanFields.isEmpty() && otherFields.isEmpty()
                    && sourceFields.isEmpty() ? null : this;
        }

        private void initAnnotatedMethods(List<Method> annotated) {
            for (Method m : annotated) {
                ProbeSource source = m.getAnnotation(ProbeSource.class);
                if (source != null) {
                    if (level == ProbeLevel.MANDATORY) {
                        sourceMethods.add(m.getName(), m);
                    }
                } else {
                    Probe probe = m.getAnnotation(Probe.class);
                    if (isProbed(level, probe)) {
                        methods.add(probeName(probe, m), m);
                    }
                }
            }
        }

        private void initAnnotatedFields(List<Field> annotated) {
            for (Field f : annotated) {
                ProbeSource source = f.getAnnotation(ProbeSource.class);
                if (source != null) {
                    if (level == ProbeLevel.MANDATORY) {
                        sourceFields.add(f.getName(), f);
                    }
                } else {
                    Probe probe = f.getAnnotation(Probe.class);
                    if (isProbed(level, probe)) {
                        initAnnotatedField(f, probe);
                    }
                }
            }
        }

        private void initAnnotatedField(Field f, Probe probe) {
            String name = probeName(probe, f);
            Class<?> valueType = f.getType();
            if (valueType.isPrimitive()) {
                if (valueType == double.class || valueType == float.class) {
                    doubleFields.add(name, f);
                } else if (valueType == boolean.class) {
                    booleanFields.add(name, f);
                } else {
                    longFields.add(name, f);
                }
            } else {
                otherFields.add(name, f);
            }
        }

        private static int countProbeSources(ProbeLevel level, List<? extends AnnotatedElement> members) {
            if (level != ProbeLevel.MANDATORY) {
                return 0;
            }
            int c = 0;
            for (AnnotatedElement e : members) {
                if (e.getAnnotation(ProbeSource.class) != null) {
                    c++;
                }
            }
            return c;
        }

        private static int countMethodProbesWith(ProbeLevel level, List<Method> probes) {
            int c = 0;
            for (Method m : probes) {
                if (!m.isAnnotationPresent(ProbeSource.class) && isProbed(level, m.getAnnotation(Probe.class))) {
                    c++;
                }
            }
            return c;
        }

        private static int countFieldProbesWith(ProbeLevel level, List<Field> probes, Class<?>... ofTypes) {
            int c = 0;
            for (Field f : probes) {
                if (!f.isAnnotationPresent(ProbeSource.class)
                        && isProbed(level, f.getAnnotation(Probe.class))
                        && contains(ofTypes, f.getType())) {
                    c++;
                }
            }
            return c;
        }

        private static boolean isProbed(ProbeLevel level, Probe probe) {
            return probe == null || probe.level() == level;
        }

        private static boolean contains(Class<?>[] types, Class<?> type) {
            if (types.length == 0) {
                return !type.isPrimitive();
            }
            for (Class<?> t : types) {
                if (t == type) {
                    return true;
                }
            }
            return false;
        }

        void collectAllProbes(CollectionCycleImpl cycle, Object instance) {
            collectLongFields(cycle, instance);
            collectDoubleFields(cycle, instance);
            collectBooleanFields(cycle, instance);
            collectOtherFields(cycle, instance);
            collectOtherMethods(cycle, instance);
            cycle.gauge = null;
        }

        void collectAllSources(CollectionCycleImpl cycle, Object instance) {
            collectSourceFields(cycle, instance);
            collectSourceMethods(cycle, instance);
        }

        private void collectSourceFields(CollectionCycle cycle, Object instance) {
            if (!sourceFields.isEmpty()) {
                for (int i = 0; i < sourceFields.size(); i++) {
                    Object value = null;
                    try {
                        value = sourceFields.values[i].get(instance);
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read field probe source", e);
                    }
                    cycle.collectAll(value);
                }
            }
        }

        private void collectSourceMethods(CollectionCycleImpl cycle, Object instance) {
            if (!sourceMethods.isEmpty()) {
                for (int i = 0; i < sourceMethods.size(); i++) {
                    cycle.collectAll(invoke(sourceMethods.values[i], instance));
                }
            }
        }

        private void collectOtherFields(CollectionCycleImpl cycle, Object instance) {
            if (!otherFields.isEmpty()) {
                for (int i = 0; i < otherFields.size(); i++) {
                    try {
                        Field field = otherFields.values[i];
                        cycle.gauge = field;
                        cycle.collect(level, otherFields.names[i], ProbeUtils.toLong(field.get(instance)));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read field probe", e);
                    }
                }
            }
        }

        private void collectBooleanFields(CollectionCycleImpl cycle, Object instance) {
            if (!booleanFields.isEmpty()) {
                for (int i = 0; i < booleanFields.size(); i++) {
                    try {
                        Field field = booleanFields.values[i];
                        cycle.gauge = field;
                        cycle.collect(level, booleanFields.names[i], field.getBoolean(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read boolean field probe", e);
                    }
                }
            }
        }

        private void collectDoubleFields(CollectionCycleImpl cycle, Object instance) {
            if (!doubleFields.isEmpty()) {
                for (int i = 0; i < doubleFields.size(); i++) {
                    try {
                        Field field = doubleFields.values[i];
                        cycle.gauge = field;
                        cycle.collect(level, doubleFields.names[i], field.getDouble(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read double field probe", e);
                    }
                }
            }
        }

        private void collectLongFields(CollectionCycleImpl cycle, Object instance) {
            if (!longFields.isEmpty()) {
                for (int i = 0; i < longFields.size(); i++) {
                    try {
                        Field field = longFields.values[i];
                        cycle.gauge = field;
                        cycle.collect(level, longFields.names[i], field.getLong(instance));
                    } catch (Exception e) {
                        LOGGER.warning("Failed to read long field probe", e);
                    }
                }
            }
        }

        private void collectOtherMethods(CollectionCycleImpl cycle, Object instance) {
            if (!methods.isEmpty()) {
                for (int i = 0; i < methods.size(); i++) {
                    Method method = methods.values[i];
                    cycle.gauge = method;
                    cycle.collect(level, methods.names[i], ProbeUtils.toLong(invoke(method, instance)));
                }
            }
        }

        private static Object invoke(Method method, Object obj) {
            try {
                return method.invoke(obj, EMPTY_ARGS);
            } catch (InvocationTargetException e) {
                LOGGER.warning(
                        "Probed method `" + method.getName() + "` throw exception:",
                        e.getTargetException());
            } catch (Exception e) {
                LOGGER.warning("Failed to read method probe: " + method.getName(), e);
            }
            return null;
        }
    }

    /**
     * Meta-data for a type that {@link Probe} annotated fields or methods.
     */
    private static final class ProbeAnnotatedType {

        final Class<?> type;
        final Method update;
        final long updateIntervalMs;
        final AtomicLong nextUpdateTimeMs = new AtomicLong();
        final ProbeLevel updateLevel;
        final boolean isUpdated;
        final boolean isSource;
        final boolean isContext;
        final boolean isAnnotated;
        final boolean isContributing;
        final ProbeAnnotatedTypeLevel[] levels;

        ProbeAnnotatedType(Class<?> type) {
            this(type, initByAnnotations(type));
        }

        ProbeAnnotatedType(Class<?> type, ProbeLevel level, String... methodNames) {
            this(type, initByNameList(type, level, methodNames));
        }

        private ProbeAnnotatedType(Class<?> type, ProbeAnnotatedTypeLevel[] levels) {
            this.type = type;
            this.levels = levels;
            this.isAnnotated = isAnnotated();
            this.isContext = ObjectMetricsContext.class.isAssignableFrom(type);
            this.isSource = MetricsSource.class.isAssignableFrom(type);

            this.update = reprobeFor(type);
            this.isUpdated = update != null;
            if (isUpdated) {
                BeforeCollectionCycle reprobe = update.getAnnotation(BeforeCollectionCycle.class);
                updateIntervalMs = ProbeUtils.updateInterval(reprobe.value(), reprobe.unit());
                updateLevel = reprobe.level();
            } else {
                updateIntervalMs = -1L;
                updateLevel = ProbeLevel.DEBUG;
            }
            this.isContributing = isSource || isAnnotated || isUpdated;
        }

        private void updateIfNeeded(Object obj) {
            long next = nextUpdateTimeMs.get();
            long now = Clock.currentTimeMillis();
            if (now > next && nextUpdateTimeMs.compareAndSet(next, now + updateIntervalMs)) {
                try {
                    update.invoke(obj, EMPTY_ARGS);
                } catch (Exception e) {
                    LOGGER.warning("Failed to update source: "
                            + update.getDeclaringClass().getSimpleName() + "."
                            + update.getName(), e);
                }
            }
        }

        private static Method reprobeFor(Class<?> type) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isAnnotationPresent(BeforeCollectionCycle.class)) {
                    m.setAccessible(true);
                    return m;
                }
            }
            return null;
        }

        private boolean isAnnotated() {
            for (int i = 0; i < levels.length; i++) {
                if (levels[i] != null) {
                    return true;
                }
            }
            return false;
        }

        private static ProbeAnnotatedTypeLevel[] initByNameList(Class<?> type, ProbeLevel level, String... methodNames) {
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
            ProbeAnnotatedTypeLevel[] res = new ProbeAnnotatedTypeLevel[ProbeLevel.values().length];
            res[level.ordinal()] = new ProbeAnnotatedTypeLevel(level, probedMethods,
                    Collections.<Field>emptyList()).tidy();
            return res;
        }

        private static ProbeAnnotatedTypeLevel[] initByAnnotations(Class<?> type) {
            List<Field> probedFields = findProbedFields(type);
            List<Method> probedMethods = findProbedMethods(type);
            removeMethodProbesOverridenByFieldProbes(probedFields, probedMethods);
            ProbeLevel[] levels = ProbeLevel.values();
            ProbeAnnotatedTypeLevel[] res = new ProbeAnnotatedTypeLevel[levels.length];
            if (!probedMethods.isEmpty() || !probedFields.isEmpty()) {
                for (ProbeLevel level : levels) {
                    res[level.ordinal()] = new ProbeAnnotatedTypeLevel(level, probedMethods,
                            probedFields).tidy();
                }
            }
            return res;
        }

        private static void removeMethodProbesOverridenByFieldProbes(List<Field> probedFields, List<Method> probedMethods) {
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

        void collectAll(CollectionCycleImpl cycle, Object instance) {
            if (!isContributing) {
                return;
            }
            try {
                if (isUpdated && cycle.isCollected(updateLevel)) {
                    updateIfNeeded(instance);
                }
                if (isAnnotated) {
                    if (isContext) {
                        collectAllInContext(cycle, instance);
                    } else {
                        collectAllInternal(cycle, instance);
                    }
                }
                if (isSource) {
                    collectAllOfSource(cycle, (MetricsSource) instance);
                }
            } catch (Exception e) {
                LOGGER.warning("Exception while collecting object of type "
                        + instance.getClass().getSimpleName(), e);
            }
        }

        private void collectAllOfSource(CollectionCycleImpl cycle, MetricsSource source) {
            if (source == null) {
                return;
            }
            int baseIndex = cycle.tagBaseIndex;
            CharSequence lastTag = cycle.lastTagName;
            cycle.tagBaseIndex = cycle.tags.length();
            source.collectAll(cycle);
            cycle.tagBaseIndex = baseIndex;
            cycle.lastTagName = lastTag;
            cycle.tags.setLength(cycle.tagBaseIndex);
        }

        /**
         * When instance itself is adding tags the tag context has to be restored as
         * well.
         */
        private void collectAllInContext(CollectionCycleImpl cycle, Object instance) {
            CharSequence lastTagName = cycle.lastTagName;
            int lastTagValuePosition = cycle.lastTagValuePosition;
            collectAllInternal(cycle, instance);
            cycle.lastTagName = lastTagName;
            cycle.lastTagValuePosition = lastTagValuePosition;
        }

        private void collectAllInternal(CollectionCycleImpl cycle, Object instance) {
            int len0 = cycle.tags.length();
            if (isContext) {
                ((ObjectMetricsContext) instance).switchToObjectContext(cycle);
            }
            for (int i = 0; i < levels.length; i++) {
                ProbeAnnotatedTypeLevel l = levels[i];
                if (l != null && cycle.isCollected(l.level)) {
                    l.collectAllProbes(cycle, instance);
                }
            }
            for (int i = 0; i < levels.length; i++) {
                ProbeAnnotatedTypeLevel l = levels[i];
                if (l != null && cycle.isCollected(l.level)) {
                    l.collectAllSources(cycle, instance);
                }
            }
            cycle.tags.setLength(len0);
        }

    }

    static <T> void sort(String[] names, T[] values) {
        if (names == null || names.length == 0) {
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
