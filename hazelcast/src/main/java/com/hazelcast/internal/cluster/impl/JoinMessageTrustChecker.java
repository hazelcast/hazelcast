package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.MulticastConfig;

import java.util.Set;

import static com.hazelcast.util.AddressUtil.matchAnyInterface;

/**
 * Check if a received join message is to be trusted.
 *
 * To be precise it's checking if IP of the JoinMessage sender
 * is among configured {@link MulticastConfig#getTrustedInterfaces()}
 *
 * When no trusted interfaces were explicitly configured then all messages are deemed
 * as trusted.
 *
 */
final class JoinMessageTrustChecker {
    private final Set<String> trustedInterfaces;

    JoinMessageTrustChecker(Set<String> trustedInterfaces) {
        this.trustedInterfaces = trustedInterfaces;
    }

    boolean isTrusted(JoinMessage joinMessage) {
        String host = joinMessage.getAddress().getHost();
        return trustedInterfaces.isEmpty() || matchAnyInterface(host, trustedInterfaces);
    }
}
