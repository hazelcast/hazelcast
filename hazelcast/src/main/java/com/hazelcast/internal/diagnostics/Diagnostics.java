/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.diagnostics.DiagnosticsOutputType.FILE;
import static com.hazelcast.internal.diagnostics.DiagnosticsPlugin.DISABLED;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The {@link Diagnostics} is a debugging tool that provides insight in all kinds
 * of potential performance and stability issues. The actual logic to provide such
 * insights, is placed in the {@link DiagnosticsPlugin}.
 */
@SuppressWarnings("WeakerAccess")
public class Diagnostics {

    /**
     * Use the {@link Diagnostics} to see internal performance metrics and cluster
     * related information.
     * <p>
     * The performance monitor logs all metrics into the log file.
     * <p>
     * For more detailed information, please check the METRICS_LEVEL.
     * <p>
     * The default is {@code false}.
     */
    public static final HazelcastProperty ENABLED = new HazelcastProperty("hazelcast.diagnostics.enabled", false);

    /**
     * The {@link DiagnosticsLogFile} uses a rolling file approach to prevent
     * eating too much disk space.
     * <p>
     * This property sets the maximum size in MB for a single file.
     * <p>
     * Every HazelcastInstance will get its own history of log files.
     * <p>
     * The default is 50.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final HazelcastProperty MAX_ROLLED_FILE_SIZE_MB
            = new HazelcastProperty("hazelcast.diagnostics.max.rolled.file.size.mb", 50);
    /**
     * The {@link DiagnosticsLogFile} uses a rolling file approach to prevent
     * eating too much disk space.
     * <p>
     * This property sets the maximum number of rolling files to keep on disk.
     * <p>
     * The default is 10.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final HazelcastProperty MAX_ROLLED_FILE_COUNT
            = new HazelcastProperty("hazelcast.diagnostics.max.rolled.file.count", 10);

    /**
     * Configures if the epoch time should be included in the 'top' section.
     * This makes it easy to determine the time in epoch format and prevents
     * needing to parse the date-format section. The default is {@code true}.
     */
    public static final HazelcastProperty INCLUDE_EPOCH_TIME = new HazelcastProperty("hazelcast.diagnostics.include.epoch", true);

    /**
     * Configures the output directory of the performance log files.
     * <p>
     * Defaults to the 'user.dir'.
     */
    public static final HazelcastProperty DIRECTORY
            = new HazelcastProperty("hazelcast.diagnostics.directory", "" + System.getProperty("user.dir"));

    /**
     * Configures the prefix for the diagnostics file.
     * <p>
     * So instead of having e.g. 'diagnostics-...log' you get 'foobar-diagnostics-...log'.
     */
    public static final HazelcastProperty FILENAME_PREFIX
            = new HazelcastProperty("hazelcast.diagnostics.filename.prefix");

    /**
     * Configures the output for the diagnostics. The default value is
     * {@link DiagnosticsOutputType#FILE} which is a set of files managed by the
     * Hazelcast process.
     */
    public static final HazelcastProperty OUTPUT_TYPE = new HazelcastProperty("hazelcast.diagnostics.stdout", FILE);

    final AtomicReference<DiagnosticsPlugin[]> staticTasks = new AtomicReference<>(new DiagnosticsPlugin[0]);
    final String baseFileName;
    final ILogger logger;
    final LoggingService loggingService;
    final String hzName;
    final HazelcastProperties properties;
    final boolean includeEpochTime;
    final File directory;

    DiagnosticsLog diagnosticsLog;

    private final ConcurrentMap<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin> pluginsMap = new ConcurrentHashMap<>();
    private final boolean enabled;
    private final DiagnosticsOutputType outputType;

    private ScheduledExecutorService scheduler;

    public Diagnostics(String baseFileName, LoggingService loggingService, String hzName, HazelcastProperties properties) {
        String optionalPrefix = properties.getString(FILENAME_PREFIX);
        this.baseFileName = optionalPrefix == null ? baseFileName : optionalPrefix + "-" + baseFileName;
        this.logger = loggingService.getLogger(Diagnostics.class);
        this.loggingService = loggingService;
        this.hzName = hzName;
        this.properties = properties;
        this.includeEpochTime = properties.getBoolean(INCLUDE_EPOCH_TIME);
        this.directory = new File(properties.getString(DIRECTORY));
        this.enabled = properties.getBoolean(ENABLED);
        this.outputType = properties.getEnum(OUTPUT_TYPE, DiagnosticsOutputType.class);
    }

    // just for testing (returns the current file the system is writing to)
    public File currentFile() throws UnsupportedOperationException {
        if (outputType != FILE) {
            throw new UnsupportedOperationException();
        }
        return ((DiagnosticsLogFile) diagnosticsLog).file;
    }

    /**
     * Gets the plugin for a given plugin class. This method should be used if
     * the plugin instance is required within some data-structure outside of the
     * Diagnostics.
     *
     * @param pluginClass the class of the DiagnosticsPlugin
     * @param <P>         type of the plugin
     * @return the DiagnosticsPlugin found, or {@code null} if not active
     */
    @SuppressWarnings("unchecked")
    public <P extends DiagnosticsPlugin> P getPlugin(Class<P> pluginClass) {
        return (P) pluginsMap.get(pluginClass);
    }

    /**
     * Registers a {@link DiagnosticsPlugin}.
     * <p>
     * This method is thread-safe.
     * <p>
     * There is no checking for duplicate registration.
     * <p>
     * If the {@link Diagnostics} is disabled, the call is ignored.
     *
     * @param plugin the plugin to register
     * @throws NullPointerException if plugin is {@code null}
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
            logger.info(format("Diagnostics disabled. To enable add -D%s=true to the JVM arguments.", ENABLED.getName()));
            return;
        }

        this.diagnosticsLog = outputType.newLog(this);
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
                diagnosticsLog.write(plugin);
            } catch (Throwable t) {
                // we need to catch any exception; otherwise the task is going to be removed by the scheduler
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
