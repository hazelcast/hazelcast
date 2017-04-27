/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.diagnostics.DiagnosticsPlugin.DISABLED;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The {@link Diagnostics} is a debugging tool that provides insight in all kinds of potential performance and stability issues.
 * The actual logic to provide such insights, is placed in the {@link DiagnosticsPlugin}.
 */
public class Diagnostics {

    public static final String PREFIX = "hazelcast.diagnostics";

    /**
     * The minimum level for probes is MANDATORY, but it can be changed to INFO or DEBUG. A lower level will increase
     * memory usage (probably just a few 100KB) and provides much greater detail on what is going on inside a HazelcastInstance.
     * <p/>
     * By default only mandatory probes are being tracked
     */
    public static final HazelcastProperty METRICS_LEVEL =
            new HazelcastProperty(PREFIX + ".metric.level", ProbeLevel.MANDATORY.name())
                    .setDeprecatedName("hazelcast.performance.metric.level");
    /**
     * Use the {@link Diagnostics} to see internal performance metrics and cluster related information.
     * <p/>
     * The performance monitor logs all metrics into the log file.
     * <p/>
     * For more detailed information, please check the METRICS_LEVEL.
     * <p/>
     * The default is false.
     */
    public static final HazelcastProperty ENABLED = new HazelcastProperty(PREFIX + ".enabled", false)
            .setDeprecatedName("hazelcast.performance.monitoring.enabled");

    /**
     * The {@link DiagnosticsLogFile} uses a rolling file approach to prevent eating too much disk space.
     * <p/>
     * This property sets the maximum size in MB for a single file.
     * <p/>
     * Every HazelcastInstance will get its own history of log files.
     * <p/>
     * The default is 10.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final HazelcastProperty MAX_ROLLED_FILE_SIZE_MB = new HazelcastProperty(PREFIX + ".max.rolled.file.size.mb", 10)
            .setDeprecatedName("hazelcast.performance.monitor.max.rolled.file.size.mb");

    /**
     * The {@link DiagnosticsLogFile} uses a rolling file approach to prevent eating too much disk space.
     * <p/>
     * This property sets the maximum number of rolling files to keep on disk.
     * <p/>
     * The default is 10.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final HazelcastProperty MAX_ROLLED_FILE_COUNT = new HazelcastProperty(PREFIX + ".max.rolled.file.count", 10)
            .setDeprecatedName("hazelcast.performance.monitor.max.rolled.file.count");

    /**
     * Determines if a human friendly, but more difficult to parse, output format is selected for dumping the metrics.
     * <p/>
     * The default is true.
     */
    public static final HazelcastProperty HUMAN_FRIENDLY_FORMAT = new HazelcastProperty(PREFIX + ".human.friendly.format", true)
            .setDeprecatedName("hazelcast.performance.monitor.human.friendly.format");

    /**
     * Configures the output directory of the performance log files.
     *
     * Defaults to the 'user.dir'.
     */
    public static final HazelcastProperty DIRECTORY
            = new HazelcastProperty(PREFIX + ".directory", "" + System.getProperty("user.dir"));

    final boolean singleLine;
    final HazelcastProperties properties;
    final String directory;
    DiagnosticsLogFile diagnosticsLogFile;
    final AtomicReference<DiagnosticsPlugin[]> staticTasks = new AtomicReference<DiagnosticsPlugin[]>(
            new DiagnosticsPlugin[0]
    );

    final ILogger logger;
    final String fileName;
    private final String hzName;
    private final boolean enabled;
    private ScheduledExecutorService scheduler;
    private final ConcurrentMap<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin> pluginsMap
            = new ConcurrentHashMap<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin>();

    public Diagnostics(String fileName, ILogger logger, String hzName, HazelcastProperties properties) {
        this.fileName = fileName;
        this.logger = logger;
        this.hzName = hzName;
        this.properties = properties;
        this.enabled = properties.getBoolean(ENABLED);
        this.directory = properties.getString(DIRECTORY);
        this.singleLine = !properties.getBoolean(HUMAN_FRIENDLY_FORMAT);
    }

    // just for testing. Returns the current file the system is writing to.
    public File currentFile() {
        return diagnosticsLogFile.file;
    }

    /**
     * Gets the plugin for a given plugin class. This method should be used if the plugin instance is required within
     * some data-structure outside of the Diagnostics.
     *
     * @param pluginClass the class of the DiagnosticsPlugin
     * @param <P>
     * @return the DiagnosticsPlugin found, or null if not active.
     */
    public <P extends DiagnosticsPlugin> P getPlugin(Class<P> pluginClass) {
        return (P) pluginsMap.get(pluginClass);
    }

    /**
     * Registers a {@link DiagnosticsPlugin}.
     *
     * This method is threadsafe.
     *
     * There is no checking for duplicate registration.
     *
     * If the {@link Diagnostics} is disabled, the call is ignored.
     *
     * @param plugin the plugin to register
     * @throws NullPointerException if plugin is null.
     */
    public void register(DiagnosticsPlugin plugin) {
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

        pluginsMap.put(plugin.getClass(), plugin);

        plugin.onStart();

        if (periodMillis > 0) {
            // it is a periodic task
            scheduler.scheduleAtFixedRate(new WritePluginTask(plugin), 0, periodMillis, MILLISECONDS);
        } else {
            addStaticPlugin(plugin);
        }
    }

    private void addStaticPlugin(DiagnosticsPlugin plugin) {
        for (; ; ) {
            DiagnosticsPlugin[] oldPlugins = staticTasks.get();
            DiagnosticsPlugin[] newPlugins = new DiagnosticsPlugin[oldPlugins.length + 1];
            arraycopy(oldPlugins, 0, newPlugins, 0, oldPlugins.length);
            newPlugins[oldPlugins.length] = plugin;
            if (staticTasks.compareAndSet(oldPlugins, newPlugins)) {
                break;
            }
        }
    }

    public void start() {
        if (!enabled) {
            logger.finest("Diagnostics is disabled");
            return;
        }

        this.diagnosticsLogFile = new DiagnosticsLogFile(this);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new DiagnosticSchedulerThreadFactory());

        logger.info("Diagnostics started");
    }

    public void shutdown() {
        if (!enabled) {
            return;
        }

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private class WritePluginTask implements Runnable {

        private final DiagnosticsPlugin plugin;

        WritePluginTask(DiagnosticsPlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public void run() {
            try {
                diagnosticsLogFile.write(plugin);
            } catch (Throwable t) {
                // we need to catch any exception; otherwise the task is going to be removed by the scheduler.
                logger.severe(t);
            }
        }
    }

    private class DiagnosticSchedulerThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable target) {
            return new Thread(target, createThreadName(hzName, "DiagnosticsSchedulerThread"));
        }
    }
}
