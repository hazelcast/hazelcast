/**
 * 
 */
package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Instance;

interface HazelcastInstanceAwareInstance extends Instance, HazelcastInstanceAware {
}