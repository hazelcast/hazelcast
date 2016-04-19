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

import static com.hazelcast.internal.monitors.PerformanceMonitorPlugin.DISABLED;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The PerformanceMonitor is a debugging tool that provides insight in all kinds of potential performance and stability issues.
 * The actual logic to provide such insights, is responsibility of the {@link PerformanceMonitorPlugin}.
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
     * Configures the output directory of the performance log files. The directory is not automatically created.
     *
     * Defaults to the 'user.dir'.
     */
    public static final HazelcastProperty DIRECTORY
            = new HazelcastProperty("hazelcast.performance.monitor.directory", "" + System.getProperty("user.dir"));

    /**
     * By default the PerformanceMonitor will output to a dedicated file. However in some cases this is not an option
     * and the one can only write to the existing logfile.
     */
    public static final HazelcastProperty SKIP_FILE
            = new HazelcastProperty("hazelcast.performance.monitor.skip.file", false);

    final HazelcastProperties properties;
    PerformanceLog performanceLog;

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

        if (enabled) {
            logger.info("PerformanceMonitor is enabled");
        }
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
                    + "The former property will be removed in Hazelcast 3.8.");
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
    public synchronized void register(final PerformanceMonitorPlugin plugin) {
        checkNotNull(plugin, "plugin can't be null");

        if (!enabled) {
            return;
        }

        start();

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
            scheduler.scheduleAtFixedRate(new RenderPluginTask(plugin), 0, periodMillis, MILLISECONDS);
        } else {
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    performanceLog.addStaticPlugin(plugin);
                }
            });
        }
    }

    private void start() {
        if (scheduler != null) {
            return;
        }

        this.performanceLog = properties.getBoolean(SKIP_FILE)
                ? new PerformanceLogLogger(this)
                : new PerformanceLogFile(this);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new PerformanceMonitorThreadFactory());
        logger.info("PerformanceMonitor started");
    }

    public synchronized void shutdown() {
        if (!enabled) {
            return;
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        // todo: closing should be done from scheduler.
        performanceLog.close();
    }

    private class RenderPluginTask implements Runnable {

        private final PerformanceMonitorPlugin plugin;

        RenderPluginTask(PerformanceMonitorPlugin plugin) {
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
