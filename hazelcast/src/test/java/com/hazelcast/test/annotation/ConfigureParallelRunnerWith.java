package com.hazelcast.test.annotation;

import com.hazelcast.test.ParallelRunnerOptions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies a class to configure {@link com.hazelcast.test.HazelcastParallelClassRunner} with
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ConfigureParallelRunnerWith {

    /**
     * @return the options class
     */
    Class<? extends ParallelRunnerOptions> value();
}
