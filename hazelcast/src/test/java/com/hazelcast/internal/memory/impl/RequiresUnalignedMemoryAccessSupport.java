package com.hazelcast.internal.memory.impl;

import com.hazelcast.test.AutoRegisteredTestRule;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(ElementType.METHOD)
@AutoRegisteredTestRule(testRule = TestIgnoreRuleAccordingToUnalignedMemoryAccessSupport.class)
public @interface RequiresUnalignedMemoryAccessSupport {
}
