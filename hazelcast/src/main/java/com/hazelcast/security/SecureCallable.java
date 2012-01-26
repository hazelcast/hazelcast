package com.hazelcast.security;

import com.hazelcast.cluster.NodeAware;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * A secure callable that runs in a sandbox.
 */
public interface SecureCallable<V> extends Callable<V>, Serializable,
        HazelcastInstanceAware, NodeAware {

}
