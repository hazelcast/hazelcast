/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.DiagnosticsConfig;
import com.hazelcast.config.DiagnosticsOutputType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * Use the {@link Diagnostics} to see internal performance metrics and cluster
     * related information.
     * <p>
     * The performance monitor logs all metrics into the log file.
     * <p>
     * For more detailed information, please check the METRICS_LEVEL.
     * <p>
     * The default is {@code false}.
     */
    @Deprecated(since = "6.0")
    public static final HazelcastProperty ENABLED = new HazelcastProperty("hazelcast.diagnostics.enabled", false);

    /**
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * The {@link DiagnosticsLogFile} uses a rolling file approach to prevent
     * eating too much disk space.
     * <p>
     * This property sets the maximum size in MB for a single file.
     * <p>
     * Every HazelcastInstance will get its own history of log files.
     * <p>
     * The default is 50.
     */
    @Deprecated(since = "6.0")
    @SuppressWarnings("checkstyle:magicnumber")
    public static final HazelcastProperty MAX_ROLLED_FILE_SIZE_MB
            = new HazelcastProperty("hazelcast.diagnostics.max.rolled.file.size.mb", 50);
    /**
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * The {@link DiagnosticsLogFile} uses a rolling file approach to prevent
     * eating too much disk space.
     * <p>
     * This property sets the maximum number of rolling files to keep on disk.
     * <p>
     * The default is 10.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    @Deprecated(since = "6.0")
    public static final HazelcastProperty MAX_ROLLED_FILE_COUNT
            = new HazelcastProperty("hazelcast.diagnostics.max.rolled.file.count", 10);

    /**
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * Configures if the epoch time should be included in the 'top' section.
     * This makes it easy to determine the time in epoch format and prevents
     * needing to parse the date-format section. The default is {@code true}.
     */
    @Deprecated(since = "6.0")
    public static final HazelcastProperty INCLUDE_EPOCH_TIME =
            new HazelcastProperty("hazelcast.diagnostics.include.epoch", true);

    /**
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * Configures the output directory of the performance log files.
     * <p>
     * Defaults to the 'user.dir'.
     */
    @Deprecated(since = "6.0")
    public static final HazelcastProperty DIRECTORY
            = new HazelcastProperty("hazelcast.diagnostics.directory", System.getProperty("user.dir"));

    /**
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * Configures the prefix for the diagnostics file.
     * <p>
     * So instead of having e.g. 'diagnostics-...log' you get 'foobar-diagnostics-...log'.
     */
    @Deprecated(since = "6.0")
    public static final HazelcastProperty FILENAME_PREFIX
            = new HazelcastProperty("hazelcast.diagnostics.filename.prefix");

    /**
     * @deprecated Configure the diagnostics over {@link Config} with {@link DiagnosticsConfig}, over config file,
     * over environment variables or over dynamic configuration. The property is deprecated.
     * <p>
     * Configures the output for the diagnostics. The default value is
     * {@link DiagnosticsOutputType#FILE} which is a set of files managed by the
     * Hazelcast process.
     */
    @Deprecated(since = "6.0")
    public static final HazelcastProperty OUTPUT_TYPE = new HazelcastProperty("hazelcast.diagnostics.stdout",
            DiagnosticsOutputType.FILE);

    /**
     * The diagnostics service is shutdown completely,
     * so that registered plugins will stop to be working and resources will be released.
     */
    public static final int SERVICE_SHUTDOWN = -1;
    /**
     * The diagnostics service is disabled so that it won't work until it is enabled.
     */
    public static final int SERVICE_DISABLED = 0;
    /**
     * The diagnostics service is disabled so that it runs the registered plugins.
     */
    public static final int SERVICE_ENABLED = 1;
    /**
     * The diagnostics service is being restarted. This happens when new diagnostics config is set at runtime.
     */
    public static final int SERVICE_RESTARTING = 2;

    public static final String DIAGNOSTIC_PROPERTY_PREFIX = "hazelcast.diagnostics.";

    final AtomicReference<DiagnosticsPlugin[]> staticTasks = new AtomicReference<>(new DiagnosticsPlugin[0]);
    final ILogger logger;
    final LoggingService loggingService;
    final String hzName;
    final HazelcastProperties hazelcastProperties;


    DiagnosticsLog diagnosticsLog;


    // There is need for preserving order of plugins and cost of put operation is neglectable.
    private final Map<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin> pluginsMap = Collections
            .synchronizedMap(new LinkedHashMap<>());
    private final Map<DiagnosticsPlugin, ScheduledFuture<?>> pluginsFutureMap = new ConcurrentHashMap<>();
    private final AtomicInteger status = new AtomicInteger();
    private final String baseFileName;

    private DiagnosticsOutputType outputType;
    private DiagnosticsConfig config;
    private File loggingDirectory;
    private String fileName;
    private String filePrefix;
    private boolean includeEpochTime;
    private float maxRollingFileSizeMB;
    private int maxRollingFileCount;
    private ScheduledExecutorService scheduler;
    private final Object lifecycleLock = new Object();

    public Diagnostics(String baseFileName, LoggingService loggingService, String hzName,
                       HazelcastProperties properties, DiagnosticsConfig config) {

        this.logger = loggingService.getLogger(Diagnostics.class);
        this.loggingService = loggingService;
        this.hzName = hzName;
        this.hazelcastProperties = properties;
        this.baseFileName = baseFileName;
        setConfig0(config);
        copyPluginProperties(config);
    }

    public String getBaseFileName() {
        return baseFileName;
    }

    public String getFileName() {
        return fileName;
    }

    public File getLoggingDirectory() {
        return loggingDirectory;
    }

    public boolean isEnabled() {
        return status.get() == SERVICE_ENABLED;
    }

    public boolean isIncludeEpochTime() {
        return includeEpochTime;
    }

    public float getMaxRollingFileSizeMB() {
        return maxRollingFileSizeMB;
    }

    public int getMaxRollingFileCount() {
        return maxRollingFileCount;
    }

    // for testing
    String getFilePrefix() {
        return filePrefix;
    }

    private void copyPluginProperties(DiagnosticsConfig config) {

        if (config.getPluginProperties() == null || config.getPluginProperties().isEmpty()) {
            return;
        }

        for (String prop : hazelcastProperties.keySet()) {
            if (prop.startsWith(DIAGNOSTIC_PROPERTY_PREFIX)) {
                config.getPluginProperties().put(prop, hazelcastProperties.get(prop));
            }
        }
    }

    // just for testing (returns the current file the system is writing to)
    public File currentFile() throws UnsupportedOperationException {
        if (outputType != DiagnosticsOutputType.FILE) {
            throw new UnsupportedOperationException();
        }
        return ((DiagnosticsLogFile) diagnosticsLog).file;
    }

    /**
     * Gets the plugin for a given plugin class. This method should be used if
     * the plugin instance is required within some data-structure outside the
     * Diagnostics.
     *
     * @param pluginClass the class of the DiagnosticsPlugin
     * @param <P>         type of the plugin
     * @return the DiagnosticsPlugin found, or {@code null} if not active
     */
    @SuppressWarnings("unchecked")
    public <P extends DiagnosticsPlugin> P getPlugin(Class<P> pluginClass) {
        // although plugins are kept in the map while service is disabled,
        // if service is disabled, plugins are not started and not active.
        // so outsiders should not be able to get the plugin instance.
        if (isEnabled()) {
            return (P) pluginsMap.get(pluginClass);
        }
        return null;
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

        long periodMillis = plugin.getPeriodMillis();
        if (periodMillis < -1) {
            throw new IllegalArgumentException(plugin + " can't return a periodMillis smaller than -1");
        }

        // hold the plugin for possible later diagnostics service enablement at runtime.
        pluginsMap.put(plugin.getClass(), plugin);

        logger.finest(plugin.getClass() + " is " + (periodMillis == DISABLED ? "disabled" : "enabled"));
        if (periodMillis == DISABLED) {
            return;
        }

        if (status.get() == SERVICE_DISABLED) {
            return;
        }

        plugin.setProperties(config.getPluginProperties());
        plugin.onStart();

        if (periodMillis > 0) {
            // it is a periodic task
            ScheduledFuture<?> future = scheduler
                    .scheduleAtFixedRate(new WritePluginTask(plugin), 0, periodMillis, MILLISECONDS);
            pluginsFutureMap.put(plugin, future);
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
        if (status.get() == SERVICE_DISABLED) {
            logger.info("\"Diagnostics disabled. To enable set DiagnosticsConfig over instance, config file, "
                    + "Management Center or Operator.");
            return;
        }

        this.diagnosticsLog = newLog(this);
        this.scheduler = new ScheduledThreadPoolExecutor(1, new DiagnosticSchedulerThreadFactory());

        logger.info(format("Diagnostics %s", (status.get() == SERVICE_RESTARTING ? "restarted" : "started")));
    }

    /**
     * Sets diagnostics config on the service. The new config will take an effect immediately. Please note that
     * if there is diagnostics config provided over environment variables or system properties
     * it will override the config.
     * <p>
     * If the service is enabled, it can be disabled over this method with provided config.
     * </p>
     * <p>
     * If the service is disabled, it can be enabled over this method with provided config.
     * </p>
     * <p>
     * If the service and new config is enabled, current plugins will be restarted with new configuration.
     * </p>
     *
     * @param diagnosticsConfig the new diagnostics config
     * @throws IllegalStateException if there is an attempt to set config while service is restarting.
     */
    public void setConfig(DiagnosticsConfig diagnosticsConfig) {
        synchronized (lifecycleLock) {
            int currentStatus = this.status.get();
            if (currentStatus == SERVICE_SHUTDOWN) {
                return;
            }

            if (currentStatus == SERVICE_RESTARTING) {
                String msg = "Diagnostics service is restarting already. You can't set a configuration at this stage.";
                logger.warning(msg);
                // throw to inform API caller
                throw new IllegalStateException(msg);
            }

            // this is a restart
            if (diagnosticsConfig.isEnabled() && currentStatus == SERVICE_ENABLED) {
                this.status.set(SERVICE_RESTARTING);
            } else {
                this.status.set(SERVICE_DISABLED);
            }

            cancelRunningPlugins();
            closeLog();
            closeScheduler();

            setConfig0(diagnosticsConfig);

            if (status.get() == SERVICE_DISABLED) {
                logger.info("Diagnostics disabled. To enable set DiagnosticsConfig over instance, config file, "
                        + "Management Center or Operator.");
                return;
            }

            start();
            scheduleRegisteredPlugins();
        }
    }

    private void closeScheduler() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private void closeLog() {
        if (diagnosticsLog != null) {
            diagnosticsLog.close();
        }
    }

    public static DiagnosticsLog newLog(Diagnostics diagnostics) {
        // class type usage of enums cannot be used as enum. So redefined the newLog here.
        return switch (diagnostics.outputType) {
            case FILE -> new DiagnosticsLogFile(diagnostics);
            case STDOUT -> new DiagnosticsStdout(diagnostics);
            case LOGGER -> new DiagnosticsLogger(diagnostics);
        };
    }

    public void restart(DiagnosticsConfig diagnosticsConfig) {

        // set the config and restart the plugins
    }

    public void shutdown() {
        synchronized (lifecycleLock) {
            if (status.get() == SERVICE_SHUTDOWN) {
                return;
            }

            status.set(SERVICE_SHUTDOWN);

            closeLog();
            closeScheduler();
        }
    }

    //created for testing purposes
    ScheduledFuture<?> getFutureOf(Class<? extends DiagnosticsPlugin> pluginType) {
        if (pluginsMap.containsKey(pluginType)) {
            DiagnosticsPlugin plugin = pluginsMap.get(pluginType);
            return pluginsFutureMap.get(plugin);
        }
        return null;
    }

    //created for testing purposes
    @SuppressWarnings("unchecked")
    <P extends DiagnosticsPlugin> P getPluginInstance(Class<P> pluginClass) {
        return (P) pluginsMap.get(pluginClass);
    }

    @SuppressWarnings({"NPathComplexity", "java:S3776", "CyclomaticComplexity", "MethodLength"})
    private void setConfig0(DiagnosticsConfig config) {
        // override config object if properties are set
        boolean override = false;
        StringBuilder sb = new StringBuilder("Diagnostics configs overridden by property: ");

        if (hazelcastProperties.containsKey(OUTPUT_TYPE)) {
            this.outputType = hazelcastProperties.getEnum(OUTPUT_TYPE, DiagnosticsOutputType.class);
            override = true;
            sb.append(OUTPUT_TYPE.getName()).append(" = ").append(outputType);
        } else {
            this.outputType = config.getOutputType();
        }

        if (hazelcastProperties.containsKey(MAX_ROLLED_FILE_SIZE_MB)) {
            this.maxRollingFileSizeMB = hazelcastProperties.getFloat(MAX_ROLLED_FILE_SIZE_MB);
            override = true;
            sb.append(MAX_ROLLED_FILE_SIZE_MB.getName()).append(" = ").append(maxRollingFileSizeMB);
        } else {
            this.maxRollingFileSizeMB = config.getMaxRolledFileSizeInMB();
        }

        if (hazelcastProperties.containsKey(MAX_ROLLED_FILE_COUNT)) {
            this.maxRollingFileCount = hazelcastProperties.getInteger(MAX_ROLLED_FILE_COUNT);
            override = true;
            sb.append(MAX_ROLLED_FILE_COUNT.getName()).append(" = ").append(maxRollingFileCount);
        } else {
            this.maxRollingFileCount = config.getMaxRolledFileCount();
        }

        if (hazelcastProperties.containsKey(FILENAME_PREFIX)) {
            this.filePrefix = hazelcastProperties.get(FILENAME_PREFIX.getName());
            override = true;
            sb.append(FILENAME_PREFIX.getName()).append(" = ").append(filePrefix);
        } else {
            this.filePrefix = config.getFileNamePrefix();
        }

        if (hazelcastProperties.containsKey(INCLUDE_EPOCH_TIME)) {
            this.includeEpochTime = hazelcastProperties.getBoolean(INCLUDE_EPOCH_TIME);
            override = true;
            sb.append(INCLUDE_EPOCH_TIME.getName()).append(" = ").append(includeEpochTime);
        } else {
            this.includeEpochTime = config.isIncludeEpochTime();
        }

        if (hazelcastProperties.containsKey(DIRECTORY)) {
            this.loggingDirectory = new File(hazelcastProperties.get(DIRECTORY.getName()));
            override = true;
            sb.append(DIRECTORY.getName()).append(" = ").append(loggingDirectory);
        } else {
            this.loggingDirectory = new File(config.getLogDirectory());
        }

        if (hazelcastProperties.containsKey(INCLUDE_EPOCH_TIME)) {
            this.includeEpochTime = hazelcastProperties.getBoolean(INCLUDE_EPOCH_TIME);
            override = true;
            sb.append(INCLUDE_EPOCH_TIME.getName()).append(" = ").append(includeEpochTime);
        } else {
            this.includeEpochTime = config.isIncludeEpochTime();
        }

        if (hazelcastProperties.containsKey(OUTPUT_TYPE)) {
            this.outputType = hazelcastProperties.getEnum(OUTPUT_TYPE, DiagnosticsOutputType.class);
            override = true;
            sb.append(OUTPUT_TYPE.getName()).append(" = ").append(outputType);
        } else {
            this.outputType = config.getOutputType();
        }

        this.fileName = this.filePrefix == null
                ? baseFileName
                : this.filePrefix + "-" + baseFileName;

        this.config = config;
        copyPluginProperties(hazelcastProperties, config);

        if (hazelcastProperties.containsKey(ENABLED)) {
            boolean enabled = hazelcastProperties.getBoolean(ENABLED);
            if (enabled) {
                status.set(SERVICE_ENABLED);
            } else {
                status.set(SERVICE_DISABLED);
            }
            override = true;
            sb.append(ENABLED.getName()).append(" = ").append(enabled);
        } else {
            status.set(config.isEnabled() ? SERVICE_ENABLED : SERVICE_DISABLED);
        }

        if (override) {
            logger.info(sb.toString());
        }
    }

    private void scheduleRegisteredPlugins() {

        logger.finest("Scheduling the diagnostics plugins {%s}",
                String.join(", ", pluginsMap
                        .keySet()
                        .stream()
                        .map(Class::getName)
                        .toArray(String[]::new)));

        for (Map.Entry<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin> entry : pluginsMap.entrySet()) {
            register(entry.getValue());
        }
    }

    private void cancelRunningPlugins() {
        logger.finest("Canceling the diagnostics plugins.");
        for (Map.Entry<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin> entry : pluginsMap.entrySet()) {
            if (pluginsFutureMap.containsKey(entry.getValue())) {
                DiagnosticsPlugin plugin = entry.getValue();
                ScheduledFuture<?> future = pluginsFutureMap.remove(plugin);
                future.cancel(false);
                plugin.onShutdown();
            }
        }

        // cancel static tasks required for the status
        for (DiagnosticsPlugin plugin : staticTasks.get()) {
            plugin.onShutdown();
        }
    }

    private void copyPluginProperties(HazelcastProperties properties, DiagnosticsConfig config) {

        if (config.getPluginProperties() == null || config.getPluginProperties().isEmpty()) {
            return;
        }

        for (String prop : properties.keySet()) {
            if (prop.startsWith(DIAGNOSTIC_PROPERTY_PREFIX)) {
                config.getPluginProperties().put(prop, properties.get(prop));
            }
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
