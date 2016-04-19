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

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.monitors.PerformanceMonitorPlugin.DISABLED;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The PerformanceMonitor is a debugging tool that provides insight in all kinds of potential performance and stability issues.
 * The actual logic to provide such insights, is placed in the {@link PerformanceMonitorPlugin}.
 */
public class PerformanceMonitor {

    /**
     * Use the performance monitor to see internal performance metrics. Currently this is quite
     * limited since it will only show read/write events per selector and operations executed per operation-thread. But in
     * the future, all kinds of new metrics will be added.
     * <p/>
     * The performance monitor logs all metrics into the log file.
     * <p/>
     * For more detailed information, please check the PERFORMANCE_METRICS_LEVEL.
     * <p/>
     * The default is false.
     */
    public static final HazelcastProperty ENABLED
            = new HazelcastProperty("hazelcast.performance.monitor.enabled", false);

    /**
     * The PerformanceMonitor uses a rolling file approach to prevent eating too much disk space.
     * <p/>
     * This property sets the maximum size in MB for a single file.
     * <p/>
     * Every HazelcastInstance will get its own history of log files.
     * <p/>
     * The default is 10.
     */
    public static final HazelcastProperty MAX_ROLLED_FILE_SIZE_MB
            = new HazelcastProperty("hazelcast.performance.monitor.max.rolled.file.size.mb", 10);

    /**
     * The PerformanceMonitor uses a rolling file approach to prevent eating too much disk space.
     * <p/>
     * This property sets the maximum number of rolling files to keep on disk.
     * <p/>
     * The default is 10.
     */
    public static final HazelcastProperty MAX_ROLLED_FILE_COUNT
            = new HazelcastProperty("hazelcast.performance.monitor.max.rolled.file.count", 10);

    /**
     * Determines if a human friendly, but more difficult to parse, output format is selected for dumping the metrics.
     * <p/>
     * The default is true.
     */
    public static final HazelcastProperty HUMAN_FRIENDLY_FORMAT
            = new HazelcastProperty("hazelcast.performance.monitor.human.friendly.format", true);

    /**
     * Configures the output directory of the performance log files.
     *
     * Defaults to the 'user.dir'.
     */
    public static final HazelcastProperty DIRECTORY
            = new HazelcastProperty("hazelcast.performance.monitor.directory", "" + System.getProperty("user.dir"));

    final boolean singleLine;
    final HazelcastProperties properties;
    final String directory;
    PerformanceLog performanceLog;
    final AtomicReference<PerformanceMonitorPlugin[]> staticTasks = new AtomicReference<PerformanceMonitorPlugin[]>(
            new PerformanceMonitorPlugin[0]
    );

    final ILogger logger;
    final String fileName;

    private final boolean enabled;
    private ScheduledExecutorService scheduler;
    private final HazelcastThreadGroup hzThreadGroup;

    public PerformanceMonitor(
            String fileName,
            ILogger logger,
            HazelcastThreadGroup hzThreadGroup,
            HazelcastProperties properties) {
        this.fileName = fileName;
        this.hzThreadGroup = hzThreadGroup;
        this.logger = logger;
        this.properties = properties;
        this.enabled = isEnabled(properties);
        this.directory = properties.getString(DIRECTORY);

        if (enabled) {
            logger.info("PerformanceMonitor is enabled");
        }
        this.singleLine = !properties.getBoolean(HUMAN_FRIENDLY_FORMAT);
    }

    private boolean isEnabled(HazelcastProperties properties) {
        String s = properties.getString(ENABLED);
        if (s != null) {
            return properties.getBoolean(ENABLED);
        }

        // check for the old property name
        s = properties.get("hazelcast.performance.monitoring.enabled");
        if (s != null) {
            logger.warning("Don't use deprecated 'hazelcast.performance.monitoring.enabled' "
                    + "but use '" + ENABLED.getName() + "' instead. "
                    + "The former name will be removed in Hazelcast 3.8.");
        }
        return Boolean.parseBoolean(s);
    }

    /**
     * Registers a PerformanceMonitorPlugin.
     *
     * This method is threadsafe.
     *
     * There is no checking for duplicate registration.
     *
     * If the PerformanceMonitor is disabled, the call is ignored.
     *
     * @param plugin the plugin to register
     * @throws NullPointerException if plugin is null.
     */
    public void register(PerformanceMonitorPlugin plugin) {
        checkNotNull(plugin, "plugin can't be null");

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

    private class MonitorTaskRunnable implements Runnable {

        private final PerformanceMonitorPlugin plugin;

        MonitorTaskRunnable(PerformanceMonitorPlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public void run() {
            try {
                performanceLog.render(plugin);
            } catch (Throwable t) {
                // we need to catch any exception; otherwise the task is going to be removed by the scheduler.
                logger.severe(t);
            }
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
