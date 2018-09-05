package com.hazelcast.internal.probing;

import static com.hazelcast.util.StringUtil.getterIntoProperty;
import static java.lang.Math.round;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;

public final class ProbingCycleImpl implements ProbeingCycle, ProbeingCycle.Tags {

    private final Map<Class<?>, ProbeAnnotatedType> metadata = new ConcurrentHashMap<Class<?>, ProbeAnnotatedType>();
    private final StringBuilder tags = new StringBuilder();
    private final ProbeRenderer renderer;

    public ProbingCycleImpl(ProbeRenderer renderer) {
        this.renderer = renderer;
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
    public void probe(String name, long value) {
        render(name, value);
    }

    @Override
    public void probe(String name, double value) {
        render(name, toLong(value));
    }

    @Override
    public void probe(String name, int value) {
        probe(name, (long) value);
    }

    @Override
    public void probe(String name, float value) {
        probe(name, (double) value);
    }

    @Override
    public Tags openContext() {
        tags.setLength(0);
        return this;
    }

    @Override
    public Tags tag(String name, String value) {
        //TODO escape value
        if (tags.length() > 0) {
            tags.append(' ');
        }
        tags.append(name).append('=').append(value);
        return this;
    }

    private void render(String name, long value) {
        int len = tags.length();
        tag("name", name);
        renderer.render(tags, value);
        tags.setLength(len);
    }

    private static long toLong(double value) {
        return round(value * 10000d);
    }

    static final class ProbeAnnotatedType {

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

        public void probeIn(ProbeingCycle cycle, Object instance) {
            probeMethods(cycle, instance);
            probeLongFields(cycle, instance);
            probeDoubleFields(cycle, instance);
            probeOtherFields(cycle, instance);
        }

        private void probeOtherFields(ProbeingCycle cycle, Object instance) {
            if (!otherFields.isEmpty()) {
                for (Entry<String, Field> f : otherFields.entrySet()) {
                    try {
                        cycle.probe(f.getKey(), toLong(f.getValue().get(instance)));
                    } catch (Exception e) {
                        // ignore it
                    }
                }
            }
        }

        private void probeDoubleFields(ProbeingCycle cycle, Object instance) {
            if (!doubleFields.isEmpty()) {
                for (Entry<String, Field> f : doubleFields.entrySet()) {
                    try {
                        cycle.probe(f.getKey(), f.getValue().getDouble(instance));
                    } catch (Exception e) {
                        // ignore it
                    }
                }
            }
        }

        private void probeLongFields(ProbeingCycle cycle, Object instance) {
            if (!longFields.isEmpty()) {
                for (Entry<String, Field> f : longFields.entrySet()) {
                    try {
                        cycle.probe(f.getKey(), f.getValue().getLong(instance));
                    } catch (Exception e) {
                        // ignore it
                    }
                }
            }
        }

        private void probeMethods(ProbeingCycle cycle, Object instance) {
            if (!methods.isEmpty()) {
                for (Entry<String, Method> m : methods.entrySet()) {
                    try {
                        cycle.probe(m.getKey(), toLong(m.getValue().invoke(instance)));
                    } catch (Exception e) {
                        // ignore it
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
