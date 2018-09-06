package com.hazelcast.internal.probing;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import com.hazelcast.internal.probing.ProbeRegistry.ProbeSource;

/**
 * Marks the method of a {@link ProbeSource} that should be called when it is
 * time to update the probe state.
 * 
 * This can be used as an alternative when computing current values for or
 * during each {@link ProbingCycle} is too "expensive". Updating instead is
 * issued by calling the annotated method before probing when at least the the
 * specified amount of time has passed.
 */
@Retention(RUNTIME)
@Target(METHOD)
public @interface Reprobe {

    int value() default 1;

    TimeUnit unit() default TimeUnit.SECONDS;
}
