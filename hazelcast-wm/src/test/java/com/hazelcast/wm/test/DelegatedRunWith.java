package com.hazelcast.wm.test;

import org.junit.runner.Runner;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface DelegatedRunWith {

    Class<? extends Runner> value();
}

