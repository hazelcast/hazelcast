package com.hazelcast.client;

import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.EventListener;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.*;

/**
 * @mdogan 5/16/13
 */
public final class LifecycleServiceImpl implements LifecycleService {

    private final HazelcastClient client;
    private final ConcurrentMap<String, LifecycleListener> lifecycleListeners = new ConcurrentHashMap<String, LifecycleListener>();
    private final Object lifecycleLock = new Object();
    private final AtomicBoolean active = new AtomicBoolean(false);

    public LifecycleServiceImpl(HazelcastClient client) {
        this.client = client;
        final Collection<EventListener> listeners = client.getClientConfig().getListeners();
        if (listeners != null && !listeners.isEmpty()) {
            for (EventListener listener : listeners) {
                if (listener instanceof MembershipListener) {
                    addLifecycleListener((LifecycleListener) listener);
                }
            }
        }
        fireLifecycleEvent(STARTING);
    }

    private ILogger getLogger() {
        return Logger.getLogger(LifecycleService.class.getName());
    }

    public String addLifecycleListener(LifecycleListener lifecycleListener) {
        final String id = UUID.randomUUID().toString();
        lifecycleListeners.put(id, lifecycleListener);
        return id;
    }

    public boolean removeLifecycleListener(String registrationId) {
        return lifecycleListeners.remove(registrationId) != null;
    }

    private void fireLifecycleEvent(LifecycleEvent.LifecycleState lifecycleState) {
        final LifecycleEvent lifecycleEvent = new LifecycleEvent(lifecycleState);
        getLogger().log(Level.INFO, client.getName() + " is " + lifecycleEvent.getState());
        for (LifecycleListener lifecycleListener : lifecycleListeners.values()) {
            lifecycleListener.stateChanged(lifecycleEvent);
        }
    }

    void setStarted() {
        active.set(true);
        fireLifecycleEvent(STARTED);
    }

    public boolean isRunning() {
        return active.get();
    }

    public void shutdown() {
        active.set(false);
        synchronized (lifecycleLock) {
            fireLifecycleEvent(SHUTTING_DOWN);
            client.shutdown();
            fireLifecycleEvent(SHUTDOWN);
        }
    }

    public void kill() {
        shutdown();
    }
}
