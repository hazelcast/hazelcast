package com.hazelcast.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is for marking template interfaces to be processed
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GenerateParameters {
    short id();

    String name();

    String ns();

    boolean generateMessageTypeEnum() default true;
}

