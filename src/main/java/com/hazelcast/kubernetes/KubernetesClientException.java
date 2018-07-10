package com.hazelcast.kubernetes;

import com.hazelcast.core.HazelcastException;

class KubernetesClientException
        extends HazelcastException {
    public KubernetesClientException(String message) {
        super(message);
    }

    public KubernetesClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
