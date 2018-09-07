package com.hazelcast.internal.probing;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;

public interface ProbingCycle {

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
     * @param prefix prefix to use for all names (without dot)
     * @param instance a obj with fields or methods annotated with {@link Probe}
     */
    void probe(CharSequence prefix, Object instance);

    void probe(CharSequence name, long value);

    void probe(CharSequence name, double value);

    void probe(ProbeLevel level, CharSequence name, long value);

    void probe(ProbeLevel level, CharSequence name, double value);

    /*
     * Tagging
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

    interface Tags {

        /**
         * Append the tag to the context. If the name is identical to the last name the
         * pair is not appended but the value just changes to the new value provided.
         * This allows to setup tags that are identical for a collection first and just
         * switch the varying tag to the current value in the loop. This does not work
         * with more then one tag. In such cases the context has to be opened and
         * reconstructed for each loop iteration.
         * 
         * @param name immutable, not null
         * @param value immutable, not null
         * @return this {@link Tags} context for chaining
         */
        Tags tag(CharSequence name, CharSequence value);

        /**
         * This method is meat as a legacy support where keys are not build in terms of tags.
         * It should only be used in exceptional cases. At some point we hopefully can remove it.
         * 
         * @param s not null
         * @return this {@link Tags} context for chaining
         */
        Tags append(CharSequence s);
    }
}
