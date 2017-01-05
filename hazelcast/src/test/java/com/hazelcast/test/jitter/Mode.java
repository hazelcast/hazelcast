package com.hazelcast.test.jitter;

/**
 * Operational Modes for {@link JitterRule}
 *
 */
public enum Mode {
    /**
     * JitterRule is explicitly disabled. No jitter monitoring is performed no matter
     * what test or environment is used.
     *
     */
    DISABLED,

    /**
     * JitterRule is explicitly enabled. Failed test will report environment hiccups (if there were any)
     *
     */
    ENABLED,

    /**
     * JitterRule is enabled when running on Jenkins only.
     *
     */
    JENKINS
}
