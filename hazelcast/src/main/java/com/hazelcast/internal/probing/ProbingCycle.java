package com.hazelcast.internal.probing;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;

/**
 * For each probe measurement a {@link ProbingCycle} is passed to all
 * {@link ProbeSource}s. The {@link ProbeSource} uses the API to communicate its
 * probes and their current values. 
 * 
 * This has two steps:
 * <ol>
 * <li>{@link #openContext()} and {@link Tags#tag(CharSequence, CharSequence)}
 * is used to describe the general context of the probes. The context described
 * before acts as a "prefix" for all subsequent measurements.</li>
 * 
 * <li>{@link #probe(CharSequence, long)} and its sibling methods are used to
 * state one measurement at a time with its name and value. Alternatively to
 * stating measurements directly an instance of a type annotated with
 * {@link Probe} can be passed to {@link #probe(Object)} to measure all
 * annotated fields and methods.</li>
 * </ol>
 * 
 * Some sources might describe several of such contexts and their probes by
 * repeating the two steps:
 * 
 * <pre>
 * &#64;Override
 * public void probeIn(ProbingCycle cycle) {
 *     cycle.openContext().tag(TAG_TYPE, "x").tag(TAG_INSTANCE, "foo");
 *     cycle.probe(foo);
 *     cycle.openContext().tag(TAG_INSTANCE, "bar");
 *     cycle.probe(bar);
 * }
 * </pre>
 */
public interface ProbingCycle {

    /*
     * Probing values:
     */

    /**
     * Allows {@link ProbeSource}s to optimize their implementations by skipping
     * handling probes for {@link ProbeLevel}s that are not relevant.
     * 
     * {@link ProbeSource}s do not have to obligation to perform this check. They
     * may just probe everything providing the level for each probe. It is the
     * {@link ProbingCycle}s obligation to make sure only relevant probes are
     * considered.
     * 
     * @param level the level to check
     * @return true, if the level is relevant for this cycle, else false
     */
    boolean isProbed(ProbeLevel level);

    /**
     * The main way of rendering metrics is to render instances that have methods
     * and fields annotated with {@link Probe}. The metric name is derived from
     * field or method name or {@link Probe#name()} (if given). The metric type
     * (long or double) is derived from the field type or method return type. The
     * set of supported types with a known mapping is fix and implementation
     * dependent.
     * 
     * @param instance a obj with fields or methods annotated with {@link Probe}
     */
    void probe(Object instance);

    /**
     * Similar to {@link #probe(Object)} just that all names become
     * {@code <prefix>.<name>} instead of just {@code <name>}.
     * 
     * @param prefix prefix to use for all names (provided without dot)
     * @param instance a obj with fields or methods annotated with {@link Probe}
     */
    void probe(CharSequence prefix, Object instance);

    void probe(CharSequence name, long value);

    void probe(CharSequence name, double value);

    void probe(CharSequence name, boolean value);

    void probe(ProbeLevel level, CharSequence name, long value);

    void probe(ProbeLevel level, CharSequence name, double value);

    void probe(ProbeLevel level, CharSequence name, boolean value);


    /*
     * Describing probing context to the cycle:
     */

    /**
     * Resets the tags used by this cycle (to empty). This provides a convenient and
     * format independent way of tagging the probed metrics.
     * 
     * After context (previously the prefix) is completed using
     * {@link Tags#tag(CharSequence, CharSequence)} the {@code probe} methods are
     * used to add measurements in this context. Then context is changed to next one
     * and so forth.
     * 
     * @return the {@link Tags} abstraction to use to build the context for this
     *         cycle.
     */
    Tags openContext();

    /**
     * The reason for {@link Tags} API is to allow "describing" complex "assembled"
     * multi-part keys to the {@link ProbingCycle} without the need to create
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
         * @param name not null, only guaranteed to be stable throughout the call
         * @param value not null, only guaranteed to be stable throughout the call
         * @return this {@link Tags} context for chaining
         */
        Tags tag(CharSequence name, CharSequence value);

        /**
         * This method is meat as a legacy support where keys are not build in terms of tags.
         * It should only be used in exceptional cases. At some point we hopefully can remove it.
         * 
         * @param s not null, only guaranteed to be stable throughout the call
         * @return this {@link Tags} context for chaining
         */
        Tags append(CharSequence s);
    }
}
