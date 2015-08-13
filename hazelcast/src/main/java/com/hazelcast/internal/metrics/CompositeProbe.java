package com.hazelcast.internal.metrics;

public @interface CompositeProbe {
    String name() default "";
}
