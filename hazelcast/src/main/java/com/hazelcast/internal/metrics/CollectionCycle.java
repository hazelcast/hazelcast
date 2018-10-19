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

package com.hazelcast.internal.metrics;

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * For each probe measurement a {@link CollectionCycle} is passed to all
 * {@link MetricsSource}s. The {@link MetricsSource} uses the API to communicate its
 * probes and their current values.
 *
 * This has two steps:
 * <ol>
 * <li>{@link #switchContext()} and {@link Tags#tag(CharSequence, CharSequence)}
 * is used to describe the general context of the probes. The context described
 * before acts as a "prefix" for all subsequent measurements.</li>
 *
 * <li>{@link #collect(CharSequence, long)} and its sibling methods are used to
 * state one measurement at a time with its name and value. Alternatively to
 * stating measurements directly an instance of a type annotated with
 * {@link Probe} can be passed to {@link #collectAll(Object)} to measure all
 * annotated fields and methods.</li>
 * </ol>
 *
 * Some sources might describe several of such contexts and their probes by
 * repeating the two steps:
 *
 * <pre>
 * public void collectAll(CollectionCycle cycle) {
 *     cycle.switchContext().tag(TAG_TYPE, "x").tag(TAG_INSTANCE, "foo");
 *     cycle.collectAll(foo);
 *     cycle.switchContext().tag(TAG_INSTANCE, "bar");
 *     cycle.collectAll(bar);
 * }
 * </pre>
 */
@PrivateApi
public interface CollectionCycle {

    /*
     * Probing values:
     */

    /**
     * Allows {@link MetricsSource}s to optimize their implementations by skipping
     * handling probes for {@link ProbeLevel}s that are not relevant.
     *
     * {@link MetricsSource}s do not have to obligation to perform this check. They
     * may just probe everything providing the level for each probe. It is the
     * {@link CollectionCycle}s obligation to make sure only relevant probes are
     * considered.
     *
     * @param level the level to check
     * @return true, if the level is relevant for this cycle, else false
     */
    boolean isCollected(ProbeLevel level);

    /**
     * The main way of collecting metrics is to collect instances that have methods
     * and fields annotated with {@link Probe}. The metric name is derived from
     * field or method name or {@link Probe#name()} (if given). The metric type
     * (long or double) is derived from the field type or method return type. The
     * set of supported types with a known mapping is fix and implementation
     * dependent. s This method is also used to dynamically collect a nested
     * {@link MetricsSource}s that have not been registered as a root before.
     *
     * This method can also be used with an array of instances. This is mostly
     * useful for types that implement {@link MetricsNs} and thereby provide
     * their own context.
     *
     * The method exits with the same {@link Tags} context it is called with.
     *
     * @param obj a obj with fields or methods annotated with {@link Probe} or an
     *        array of such objects or an instance implementing
     *        {@link MetricsSource} and/or {@link MetricsNs}.
     */
    void collectAll(Object obj);

    /**
     * Similar to {@link #collectAll(Object)} just that probed methods are not identified
     * using annotations but given as a list. All methods use the specified level.
     * No fields will be probed.
     *
     * This can be used to probe beans that cannot be annotated.
     *
     * @param level the level to use for all probes created
     * @param instance the obj whose methods to probe.
     * @param methods the names of the methods to probe. Methods that do not exist
     *        or have the wrong number of arguments (not none) are ignored.
     */
    void collect(ProbeLevel level, Object instance, String[] methods);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, long)} with {@link ProbeLevel#INFO}.
     */
    void collect(CharSequence name, long value);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, double)} with {@link ProbeLevel#INFO}.
     */
    void collect(CharSequence name, double value);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, boolean)} with {@link ProbeLevel#INFO}.
     */
    void collect(CharSequence name, boolean value);

    /**
     * Collects the given key-value pair as long as {@link #isCollected(ProbeLevel)}.
     *
     * @param level the level for with the pair is gathered
     * @param name name relative to context (full key is {@code context+name})
     * @param value current value measured ({@code -1} if unknown or unspecified)
     */
    void collect(ProbeLevel level, CharSequence name, long value);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, long)} with {@link ProbeUtils#toLong(double)}.
     */
    void collect(ProbeLevel level, CharSequence name, double value);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, long)} with {@link ProbeUtils#toLong(boolean)}.
     */
    void collect(ProbeLevel level, CharSequence name, boolean value);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, long)} except that value
     * is provided by the passed {@link LongProbeFunction}.
     *
     * This is useful to allow to creation of gauges for the passed function.
     */
    void collect(ProbeLevel level, CharSequence name, LongProbeFunction value);

    /**
     * Equal to {@link #collect(ProbeLevel, CharSequence, double)} except that value
     * is provided by the passed {@link DoubleProbeFunction}.
     *
     * This is useful to allow to creation of gauges for the passed function.
     */
    void collect(ProbeLevel level, CharSequence name, DoubleProbeFunction value);

    /**
     * Used to forward metrics received from the client. This is only different from
     * {@link #collect(CharSequence, long)} except that it will not escape the name
     * but un-escaping escaped line-feeds.
     *
     * Also forwarded metrics are not filtered by level as this already happend when
     * they were collected originally.
     *
     * @param name a properly escaped name (as written before)
     * @param value value as written before (double/boolean already converted to
     *        long)
     */
    void collectForwarded(CharSequence name, long value);

    /*
     * Describing probing context to the cycle:
     */

    /**
     * Resets the tags used by this cycle to initial state for the nesting level.
     *
     * The {@link Tags} context API provides a convenient and format independent way
     * of tagging the metrics.
     *
     * After context (previously the prefix) is completed using
     * {@link Tags#tag(CharSequence, CharSequence)} the
     * {@link #collect(CharSequence, long)} and {@link #collectAll(Object)} methods
     * are used to collect metrics within this context. Then context is changed to
     * next one and so forth.
     *
     * @return the {@link Tags} abstraction to use to build the current context for
     *         this cycle that is usable until the context is switched again. This
     *         call does not create any new/litter objects.
     */
    Tags switchContext();

    /**
     * The reason for {@link Tags} API is to allow "describing" complex "assembled"
     * multi-part keys to the {@link CollectionCycle} without the need to create
     * intermediate objects like {@link String} concatenation would.
     */
    interface Tags {

        /**
         * Append the tag to the context. If the name is identical to the last name the
         * pair is not appended but the value just changes to the new value provided.
         * This allows to setup tags that are identical for a collection first and just
         * switch the varying tag to the current value in the loop. This does not work
         * with more then one tag. In such cases the context has to be opened and
         * reconstructed for each loop iteration.
         *
         * The given {@link CharSequence} for {@code value} will always be escaped.
         *
         * @param name not null, only guaranteed to be stable throughout the call
         * @param value not null, only guaranteed to be stable throughout the call
         * @return this {@link Tags} context for chaining. This call does not create any
         *         new/litter objects.
         */
        Tags tag(CharSequence name, CharSequence value);

        Tags tag(CharSequence name, CharSequence major, CharSequence minor);

        /**
         * Similar to {@link #tag(CharSequence, CharSequence)} just that value is a number.
         * Sole purpose here is to avoid the need to wrap the number in a {@link CharSequence}.
         * @param name name not null, only guaranteed to be stable throughout the call
         * @param value the tag value
         * @return this {@link Tags} context for chaining. This call does not create any
         *         new/litter objects.
         */
        Tags tag(CharSequence name, long value);

        /**
         * Same as {@link #tag(CharSequence, CharSequence)} with tag name
         * {@link MetricsSource#TAG_NAMESPACE}.
         */
        Tags namespace(CharSequence ns);

        /**
         * Same as {@link #tag(CharSequence, CharSequence, CharSequence)} with tag name
         * {@link MetricsSource#TAG_NAMESPACE}.
         */
        Tags namespace(CharSequence ns, CharSequence subSpace);

        /**
         * Same as {@link #tag(CharSequence, CharSequence)} with tag name
         * {@link MetricsSource#TAG_INSTANCE}.
         */
        Tags instance(CharSequence name);

        /**
         * Same as {@link #tag(CharSequence, CharSequence, CharSequence)} with tag name
         * {@link MetricsSource#TAG_INSTANCE}.
         */
        Tags instance(CharSequence name, CharSequence subName);
    }
}
