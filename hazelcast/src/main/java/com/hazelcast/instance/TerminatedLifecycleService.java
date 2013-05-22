package com.hazelcast.instance;

import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;

/**
* @mdogan 5/22/13
*/
public final class TerminatedLifecycleService implements LifecycleService {

    public boolean isRunning() {
        return false;
    }

    public void shutdown() {
    }

    public void kill() {
    }

    public String addLifecycleListener(LifecycleListener lifecycleListener) {
        throw new UnsupportedOperationException();
    }

    public boolean removeLifecycleListener(String registrationId) {
        throw new UnsupportedOperationException();
    }
}
