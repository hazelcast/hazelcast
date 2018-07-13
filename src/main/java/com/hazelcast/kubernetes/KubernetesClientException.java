package com.hazelcast.kubernetes;

import com.hazelcast.core.HazelcastException;

/**
 * Exception to indicate any issues with {@link KubernetesClient}.
 */
class KubernetesClientException
        extends HazelcastException {
    KubernetesClientException(String message) {
        super(message);
    }

    KubernetesClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
