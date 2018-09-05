package com.hazelcast.internal.probing;

import com.hazelcast.internal.metrics.Probe;

public interface ProbeingCycle {

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
    
    void probe(String name, long value);
    
    void probe(String name, double value);
    
    /*
     * Just for convenience 
     */
    
    void probe(String name, int value);
    
    void probe(String name, float value);
    
    /*
     * Tagging
     */
    
    interface Tags {
        
        Tags tag(String name, String value);
    }
    
    /**
     * Resets the tags used by this renderer. Subsentential calls to any of the
     * {@code render} methods assumes the context created by calls to
     * {@link Tags#tag(String, String)} since this method was called. This provides
     * a convenient and format independent way of tagging the rendered metrics.
     * 
     * @return the {@link Tags} abstraction to use to build the context for this
     *         renderer.
     */
    Tags openContext();
}
