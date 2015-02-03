package com.hazelcast.logging;

/**
 * Support class for advanced troubleshooting. It's not intended to be used under normal circumstances and
 * it is disabled by default. Set a system property {@code hazelcast.troubleshooting.tracing.enabled}
 * to {@code true} to enable tracing.
 *
 * Each probe must prefix its output to by a unique code for convenient filtering and searching.
 *
 */
public final class Tracing {
    public static final boolean ENABLED =
            Boolean.getBoolean("hazelcast.troubleshooting.tracing.enabled");


    public static final String DELAYED_INVOCATION_CODE = "TRC001";
    public static final boolean DELAYED_INVOCATION_ENABLED = ENABLED
            && !Boolean.getBoolean("hazelcast.troubleshooting.tracing.delayed.invocation.disabled");
    public static final int DELAYED_INVOCATION_THRESHOLD_MS =
            Integer.getInteger("hazelcast.troubleshooting.tracing.delayed.invocation.threshold.ms", 1000);

    public static final String PACKET_THROUGHPUT_CODE = "TRC002";
    public static final boolean PACKET_THROUGHPUT_ENABLED = ENABLED
            && !Boolean.getBoolean("hazelcast.troubleshooting.tracing.packet.throughput.disabled");
    public static final int PACKET_THROUGHPUT_SLEEP_MS =
            Integer.getInteger("hazelcast.troubleshooting.tracing.packet.throughput.delay.ms", 10000);

    public static final String CLUSTER_TIME_CODE = "TRC003";
    public static final boolean CLUSTER_TIME_ENABLED = ENABLED
            && !Boolean.getBoolean("hazelcast.troubleshooting.tracing.clustertime.disabled");
    public static final int CLUSTER_TIME_THRESHOLD_MS =
            Integer.getInteger("hazelcast.troubleshooting.tracing.clustertime.threshold.ms", 1000);

    private Tracing() {
    }
}
