package com.hazelcast.internal.query;

import com.hazelcast.cluster.Member;

public class QueryServiceMetadata {
    /** Number of threads responsible for data processing. */
    private static final String ATTR_DATA_THREADS = "hazelcast.query.data.threads";

    public static void setQueryDataThreads(Member member, int val) {
        member.setAttribute(ATTR_DATA_THREADS, Integer.toString(val));
    }

    public static int getQueryDataThreads(Member member) {
        try {
            return Integer.parseInt(member.getAttribute(ATTR_DATA_THREADS));
        }
        catch (NumberFormatException e) {
            // TODO: Proper handling.
            return 1;
        }
    }

    private QueryServiceMetadata() {
        // No-op.
    }
}
