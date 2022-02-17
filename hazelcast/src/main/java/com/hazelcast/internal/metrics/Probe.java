/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation that can be placed on a field or a method of an object to indicate
 * that it should be tracked by the MetricsRegistry when the
 * {@link MetricsRegistry#registerStaticMetrics(Object, String)} is called.
 * <p>
 * The MetricsRegistry will automatically scan all interfaces and super classes
 * of an object (recursively). So it is possible to define a Probe on e.g. an
 * interface or an abstract class.
 *
 * <h1>Prefer field</h1>
 * Prefer placing the Probe to a field rather than to a method if the type is primitive.
 * The {@link java.lang.reflect.Field} provides access to the actual field
 * without the need for autoboxing. With the {@link java.lang.reflect.Method}
 * a wrapper object is created when a primitive is returned by the method.
 * Therefore, fields produce less garbage than methods.
 * <p>
 * A Probe can be placed on fields or methods with the following (return) type:
 * <ol>
 * <li>byte</li>
 * <li>short</li>
 * <li>int</li>
 * <li>float</li>
 * <li>long</li>
 * <li>double</li>
 * <li>{@link java.util.concurrent.atomic.AtomicInteger}</li>
 * <li>{@link java.util.concurrent.atomic.AtomicLong}</li>
 * <li>{@link com.hazelcast.internal.util.counters.Counter}</li>
 * <li>{@link Byte}</li>
 * <li>{@link Short}</li>
 * <li>{@link Integer}</li>
 * <li>{@link Float}</li>
 * <li>{@link Double}</li>
 * <li>{@link java.util.Collection}: it will return the size</li>
 * <li>{@link java.util.Map}: it will return the size</li>
 * <li>{@link java.util.concurrent.Semaphore}: it will return the number of
 * available permits</li>
 * </ol>
 *
 * If the field or method points to a null reference, it is interpreted as 0.
 */
@Retention(value = RUNTIME)
@Target({FIELD, METHOD })
public @interface Probe {

    /**
     * The name of the Probe.
     *
     * @return the name of the Probe.
     */
    String name();

    /**
     * Returns the ProbeLevel.
     *
     * Using ProbeLevel one can indicate how 'important' this Probe is. This is
     * useful to reduce memory overhead due to tracking of probes.
     *
     * @return the ProbeLevel.
     */
    ProbeLevel level() default INFO;

    /**
     * Measurement unit of a Probe. Not used on member, becomes a part of the key.
     */
    ProbeUnit unit() default COUNT;

    /**
     * Returns the targets excluded for this Probe. Used for filtering in
     * {@link MetricsPublisher}s.
     * <p/>
     * The final excluded targets for a metric will be the union of the
     * exclusions defined with this annotation and the exclusions defined
     * on the {@link ExcludedMetricTargets}s annotation of the class
     * containing the given Probe.
     *
     * @return the targets excluded for this Probe
     * @see ExcludedMetricTargets
     */
    MetricTarget[] excludedTargets() default {};
}
