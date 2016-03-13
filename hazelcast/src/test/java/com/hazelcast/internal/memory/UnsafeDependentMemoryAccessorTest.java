package com.hazelcast.internal.memory;

import com.hazelcast.internal.memory.impl.TestIgnoreRuleAccordingToUnsafeAvailability;
import org.junit.ClassRule;

public abstract class UnsafeDependentMemoryAccessorTest {

    @ClassRule
    public static final TestIgnoreRuleAccordingToUnsafeAvailability UNSAFE_AVAILABILITY_RULE
            = new TestIgnoreRuleAccordingToUnsafeAvailability();

}
