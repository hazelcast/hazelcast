package com.hazelcast.internal.probing;

/**
 * A service made accessible to core services so they have a chance to register
 * the "root objects" of the system.
 * 
 * After the registration phase the set of this {@link ProbeSource}s is fix.
 * There is no need to unregister them later on as they always have the choice
 * to stop rendering something.
 * 
 * Metrics about things that change over time require a corresponding
 * {@link ProbeSource} (existing since the startup) that does probe the
 * available instances as they appear and stops doing that as they disappear.
 */
public interface ProbeRegistry {

    /**
     * Called once at startup, typically by a core service registering itself.
     * 
     * @param source a object that "knows" how to render metrics in its context
     */
    void register(ProbeSource source);

    void renderTo(ProbeRenderer renderer);

    /**
     * Implemented by "root objects" (like core services) that know about a
     * particular set of instances they want to probe.
     * 
     * These can have the form of objects with annotated fields and methods or
     * directly prove a value for a given name.
     */
    interface ProbeSource {

        /**
         * Called for each {@link ProbingCycle} asking this source to probe all its
         * metrics using the provided cycle instance.
         * 
         * @param cycle accumulating probing data
         */
        void probeIn(ProbingCycle cycle);

        String TAG_INSTANCE = "instance";
        String TAG_TYPE = "type";
    }
}
