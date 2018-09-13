package com.hazelcast.internal.probing;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;

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
@Beta
@PrivateApi
public interface ProbeRegistry {

    /**
     * Called once at startup, typically by a core service registering itself.
     * 
     * @param source a object that "knows" how to render metrics in its context
     */
    void register(ProbeSource source);

    /**
     * Legacy support instances only known by their interface with implementations
     * that possibly implement {@link ProbeSource}.
     * 
     * @param source a object possibly implementing {@link ProbeSource}, might be
     *        null as well
     */
    void registerIfSource(Object source);

    /**
     * @return Creates a new "private "context that should be kept by the caller to
     *         render the contents of this {@link ProbeRegistry}. The implementation
     *         will not support multi-threading as each thread should create its own
     *         context instance.
     */
    ProbeRenderContext newRenderingContext();

    /**
     * From a usability point of view the {@link ProbeRenderContext} is a bit
     * cumbersome and smells like over-abstraction. It is purely introduced to
     * achieve the goal of rendering without creating garbage objects. That means
     * state needs to be reused. This object is the place where state can be kept in
     * a way that allows reuse between rendering cycles.
     * 
     * The {@link ProbeRenderer} itself usually changes for each cycle as it tends
     * to be dependent on output stream objects handed to it.
     */
    interface ProbeRenderContext {

        /**
         * Causes a {@link ProbingCycle} that is directed at the given
         * {@link ProbeRenderer}.
         * 
         * This method does not support multi-threading. If potentially concurrent calls
         * to this method should be made each should originate from its own
         * {@link ProbeRenderContext}.
         * 
         * @param renderer not null; is called for each active prove with a key and
         *        value to convert them to the renderer specific format.
         */
        void renderAt(ProbeLevel level, ProbeRenderer renderer);
    }

    /**
     * Implemented by "root objects" (like core services) that know about a
     * particular set of instances they want to probe.
     * 
     * Probes can have the form of objects with {@link Probe} annotated fields or
     * methods or are directly provide a value for a given name using
     * {@link ProbingCycle#probe(CharSequence, long)} (and its sibling methods).
     * 
     * Implementations of {@link ProbeSource}s that are registered services at the
     * {@link ServiceManager} do not need explicit registration in the
     * {@link ProbeRegistry} as all services implementing the interface are
     * registered automatically at the end of the node startup.
     */
    interface ProbeSource {

        /**
         * Called for each {@link ProbingCycle} asking this source to probe all its
         * metrics using the provided cycle instance.
         * 
         * Implementations can expect a clean context and do not have to start with
         * {@link ProbingCycle#openContext()}.
         * 
         * @param cycle accumulating probing data
         */
        void probeIn(ProbingCycle cycle);

        String TAG_INSTANCE = "instance";
        String TAG_TYPE = "type";
        String TAG_TARGET = "target";
    }
}
