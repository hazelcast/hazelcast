package com.hazelcast.security;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.hazelcast.cluster.NodeAware;
import com.hazelcast.core.HazelcastInstanceAware;

/**
 * A secure callable that runs in a sandbox.
 */
public interface SecureCallable<V> extends Callable<V>, Serializable,
		HazelcastInstanceAware, NodeAware {

}
