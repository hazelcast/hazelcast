package com.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;

public class HazelcastClientManagedContext implements ManagedContext {
    
    private final HazelcastInstance instance;
    private final ManagedContext externalContext;
    private final boolean hasExternalContext;

    public HazelcastClientManagedContext(final HazelcastInstance instance, final ManagedContext externalContext) {
        this.instance = instance;
        this.externalContext = externalContext;
        hasExternalContext = this.externalContext != null;
    }

    public final Object initialize(Object obj) {
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(instance);
        }

        if (hasExternalContext) {
            obj = externalContext.initialize(obj);
        }
        return obj;
    }

}
