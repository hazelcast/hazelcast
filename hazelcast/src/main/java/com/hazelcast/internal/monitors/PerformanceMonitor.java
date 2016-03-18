/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.monitors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastProperties;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_ENABLED;
import static com.hazelcast.instance.GroupProperty.PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT;
import static com.hazelcast.internal.monitors.PerformanceMonitorPlugin.DISABLED;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The PerformanceMonitor is a debugging tool that provides insight in all kinds of potential performance and stability issues.
 * The actual logic to provide such insights, is placed in the {@link PerformanceMonitorPlugin}.
 */
public class PerformanceMonitor {

    final boolean singleLine;
    final HazelcastInstance hazelcastInstance;
    final HazelcastProperties properties;
    PerformanceLog performanceLog;
    final AtomicReference<PerformanceMonitorPlugin[]> staticTasks = new AtomicReference<PerformanceMonitorPlugin[]>(
            new PerformanceMonitorPlugin[0]
    );

    final ILogger logger;
    private final boolean enabled;
    private ScheduledExecutorService scheduler;
    private final HazelcastThreadGroup hzThreadGroup;

    public PerformanceMonitor(HazelcastInstance hazelcastInstance,
                              ILogger logger,
                              HazelcastThreadGroup hzThreadGroup,
                              HazelcastProperties properties) {
        this.hazelcastInstance = hazelcastInstance;
        this.hzThreadGroup = hzThreadGroup;
        this.logger = logger;
        this.properties = properties;
        this.enabled = isEnabled(properties);
        if (enabled) {
            logger.info("PerformanceMonitor is enabled");
        }
        this.singleLine = !properties.getBoolean(PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT);
    }

    private boolean isEnabled(HazelcastProperties properties) {
        String s = properties.getString(PERFORMANCE_MONITOR_ENABLED);
        if (s != null) {
            return properties.getBoolean(PERFORMANCE_MONITOR_ENABLED);
        }

        // check for the old property name.
        s = properties.get("hazelcast.performance.monitoring.enabled");
        if (s != null) {
            logger.warning("Don't use deprecated 'hazelcast.performance.monitoring.enabled' "
                    + "but use '" + PERFORMANCE_MONITOR_ENABLED.getName() + "' instead. "
                    + "The former name will be removed in Hazelcast 3.8.");
        }
        return Boolean.parseBoolean(s);
    }

    /**
     * Registers a MonitorTask to it will be scheduled.
     *
     * This method is threadsafe.
     *
     * There is no checking for duplicate registration.
     *
     * If the PerformanceMonitor is disabled, the call is ignored.
     *
     * @param plugin the monitorTask to register
     * @throws NullPointerException if monitorTask is null.
     */
    public void register(PerformanceMonitorPlugin plugin) {
        checkNotNull(plugin, "monitorTask can't be null");

        if (!enabled) {
            return;
        }

        long periodMillis = plugin.getPeriodMillis();
        if (periodMillis < -1) {
            throw new IllegalArgumentException(plugin + " can't return a periodMillis smaller than -1");
        }

        logger.finest(plugin.getClass().toString() + " is " + (periodMillis == DISABLED ? "disabled" : "enabled"));

        if (periodMillis == DISABLED) {
            return;
        }

        plugin.onStart();

        if (periodMillis > 0) {
            // it is a periodic task
            scheduler.scheduleAtFixedRate(new MonitorTaskRunnable(plugin), 0, periodMillis, MILLISECONDS);
        } else {
            addStaticPlugin(plugin);
        }
    }

    private void addStaticPlugin(PerformanceMonitorPlugin plugin) {
        for (; ; ) {
            PerformanceMonitorPlugin[] oldPlugins = staticTasks.get();
            PerformanceMonitorPlugin[] newPlugins = new PerformanceMonitorPlugin[oldPlugins.length + 1];
            arraycopy(oldPlugins, 0, newPlugins, 0, oldPlugins.length);
            newPlugins[oldPlugins.length] = plugin;
            if (staticTasks.compareAndSet(oldPlugins, newPlugins)) {
                break;
            }
        }
    }

    public void start() {
        if (!enabled) {
            return;
        }

        this.performanceLog = new PerformanceLog(this);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new PerformanceMonitorThreadFactory());

        logger.info("PerformanceMonitor started");
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    class MonitorTaskRunnable implements Runnable {
        private final PerformanceMonitorPlugin plugin;

        public MonitorTaskRunnable(PerformanceMonitorPlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public void run() {
            performanceLog.render(plugin);
        }
    }

    private class PerformanceMonitorThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable target) {
            return new Thread(
                    hzThreadGroup.getInternalThreadGroup(),
                    target,
                    hzThreadGroup.getThreadNamePrefix("PerformanceMonitorThread"));
        }
    }
}
