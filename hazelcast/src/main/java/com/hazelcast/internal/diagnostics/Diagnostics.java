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

import com.hazelcast.auditlog.AuditlogService;
import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiagnosticsConfig;
import com.hazelcast.config.DiagnosticsOutputType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.io.File;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.diagnostics.DiagnosticsPlugin.NOT_SCHEDULED_PERIOD_MS;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

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

    /**
     * The timeout for the diagnostics service to terminate.
     */
    public static final long TERMINATE_TIMEOUT_SECONDS = 30;

    public static final String DIAGNOSTIC_PROPERTY_PREFIX = "hazelcast.diagnostics.";

    static final int AUTO_OFF_BACKOFF_SECONDS = 5;
    final AtomicReference<DiagnosticsPlugin[]> staticTasks = new AtomicReference<>(new DiagnosticsPlugin[0]);
    final ILogger logger;
    final LoggingService loggingService;
    final String hzName;
    // Accessible for testing
    TimeUnit autoOffDurationUnit = MINUTES;
    // not final for testing purposes
    HazelcastProperties hazelcastProperties;


    DiagnosticsLog diagnosticsLog;


    // There is need for preserving order of plugins and cost of put operation is neglectable.
    private final Map<Class<? extends DiagnosticsPlugin>, DiagnosticsPlugin> pluginsMap = Collections
            .synchronizedMap(new LinkedHashMap<>());
    private final Map<DiagnosticsPlugin, ScheduledFuture<?>> pluginsFutureMap = new ConcurrentHashMap<>();
    private final AtomicInteger status = new AtomicInteger();
    private final String baseFileName;

    // each start of the diagnostics service will create a new time stamp for that "session"
    private String baseFileNameWithTime;
    private DiagnosticsOutputType outputType;
    private DiagnosticsConfig config;
    private File loggingDirectory;
    private String filePrefix;
    private boolean includeEpochTime;
    private float maxRollingFileSizeMB;
    private int maxRollingFileCount;
    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService autoOffScheduler;
    private ScheduledFuture<?> autoOffFuture;
    private final Object lifecycleLock = new Object();
    private final AuditlogService auditlogService;

    public Diagnostics(String baseFileName, LoggingService loggingService, String hzName,
                       HazelcastProperties properties, DiagnosticsConfig config) {
        this(baseFileName, loggingService, hzName, properties, config, null);
    }

    public Diagnostics(String baseFileName, LoggingService loggingService, String hzName,
                       HazelcastProperties properties, DiagnosticsConfig config, AuditlogService auditlogService) {
        this.logger = loggingService.getLogger(Diagnostics.class);
        this.loggingService = loggingService;
        this.hzName = hzName;
        this.hazelcastProperties = properties;
        this.baseFileName = baseFileName;
        this.auditlogService = auditlogService;
        setConfig0(config);
    }

    public String getBaseFileNameWithTime() {
        return baseFileNameWithTime != null
                ? baseFileNameWithTime
                : baseFileName;
    }

    public String getFileName() {
        return this.filePrefix == null
                ? getBaseFileNameWithTime()
                : this.filePrefix + "-" + getBaseFileNameWithTime();
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
     * The duplicated plugins will overwrite the previous one.
     * <p>
     * If the {@link Diagnostics} is disabled, the call is ignored.
     *
     * @param plugin the plugin to register
     * @throws NullPointerException if plugin is {@code null}
     */
    public void register(DiagnosticsPlugin plugin) {
        checkNotNull(plugin, "plugin can't be null");

        plugin.setProperties(config.getPluginProperties());
        long periodMillis = plugin.getPeriodMillis();
        if (periodMillis < -1) {
            throw new IllegalArgumentException(plugin + " can't return a periodMillis smaller than -1");
        }

        // this plugin's lifecycle managed statically. If Diagnostics enabled statically(first registration on nodeEngine
        // while service is enabled), then register it. Otherwise, it cannot be enabled or disabled later at runtime.
        if (isDynamicallyManagedPlugin(periodMillis, plugin)) {
            pluginsMap.put(plugin.getClass(), plugin);
        } else if (plugin.canBeEnabledDynamically()) {
            pluginsMap.put(plugin.getClass(), plugin);
        }

        logger.finest(plugin.getClass() + " is " + (periodMillis == NOT_SCHEDULED_PERIOD_MS ? "disabled" : "enabled"));
        if (periodMillis == NOT_SCHEDULED_PERIOD_MS) {
            return;
        }

        if (status.get() == SERVICE_DISABLED) {
            return;
        }

        schedulePlugin0(plugin, periodMillis);
    }

    private void schedulePlugin0(DiagnosticsPlugin plugin, long periodMillis) {
        try {
            plugin.onStart();
        } catch (Throwable t) {
            logger.warning("Diagnostics plugin failed to start: " + plugin, t);
            return;
        }

        if (periodMillis > 0) {
            // it is a periodic task
            ScheduledFuture<?> future = scheduler
                    .scheduleAtFixedRate(new WritePluginTask(plugin), 0, periodMillis, MILLISECONDS);
            pluginsFutureMap.put(plugin, future);
        } else {
            addStaticPlugin(plugin);
        }
    }

    private boolean isDynamicallyManagedPlugin(long periodMillis, DiagnosticsPlugin plugin) {
        return !plugin.canBeEnabledDynamically()
                && periodMillis > NOT_SCHEDULED_PERIOD_MS
                // it should be the first registration
                && !pluginsMap.containsKey(plugin.getClass())
                && isEnabled();
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

        long startedTime = currentTimeMillis();
        baseFileNameWithTime = baseFileName + "-" + startedTime;
        Instant startedTimeInstant = Instant.ofEpochMilli(startedTime);
        String message = format("Diagnostics started at [%s]-[%s] with configuration %s", startedTime,
                startedTimeInstant.atZone(ZoneOffset.UTC), config);
        logger.info(message);

        if (auditlogService != null) {
            auditlogService.eventBuilder(AuditlogTypeIds.DIAGNOSTICS_LOGGING_START)
                    .message(message)
                    .addParameter("DiagnosticsConfig", config)
                    .log();
        }

        this.diagnosticsLog = newLog(this);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DiagnosticSchedulerThreadFactory("DiagnosticsSchedulerThread"));
        scheduleAutoOff();
    }

    /**
     * Sets diagnostics config on the service. The new config will take an effect immediately. Please note that
     * if there is diagnostics config provided over environment variables or system properties
     * it will override the config.
     * <p>
     * If the service is enabled, it can be disabled over this method with provided config.
     * Unless there is a no NonDynamic Plugin, such as StoreLatencyPlugin
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
                String message = "Diagnostics is going to restart with new configuration.";
                logger.info(message);
                if (auditlogService != null) {
                    auditlogService.eventBuilder(AuditlogTypeIds.DIAGNOSTICS_LOGGING_RESTART)
                            .message(message)
                            .addParameter("DiagnosticsConfig", diagnosticsConfig)
                            .log();
                }
            } else {
                // there is a non-dynamic plugin running and the service cannot be disabled.
                if (isNonDynamicPluginExist()) {
                    this.status.set(SERVICE_RESTARTING);
                    diagnosticsConfig.setEnabled(true);
                    logNonDynamicPluginWarnings();
                } else {
                    this.status.set(SERVICE_DISABLED);
                }
            }

            cancelRunningPlugins();
            closeScheduler();
            closeLog();

            setConfig0(diagnosticsConfig);

            if (status.get() == SERVICE_DISABLED) {
                logger.info("Diagnostics disabled. To enable set DiagnosticsConfig over instance, config file, "
                        + "Management Center or Operator.");
                if (auditlogService != null) {
                    auditlogService.eventBuilder(AuditlogTypeIds.DIAGNOSTICS_LOGGING_DISABLE)
                            .message("Diagnostics disabled")
                            .addParameter("DiagnosticsConfig", config)
                            .log();
                }
                // if the service is disabled, no need to wait for the auto off timer
                cancelAutoOffFuture();
                return;
            }

            start();
            scheduleRegisteredPlugins();
        }
    }

    private void logNonDynamicPluginWarnings() {
        String nameOfNonDynamicPlugins = pluginsMap
                .entrySet()
                .stream()
                .filter(entry -> !entry.getValue().canBeEnabledDynamically())
                .map(entry -> entry.getKey().getName())
                .reduce((first, second) -> first + ", " + second)
                .orElse("none");
        logger.warning("Diagnostics cannot be disabled dynamically since there are non dynamic plugins running."
                + "They can be disabled only statically which requires node restart."
                + "The service is going to restart with new configuration. You can disable or configure other plugins."
                + "The running non dynamic plugins are: " + nameOfNonDynamicPlugins);
    }

    private boolean isNonDynamicPluginExist() {
        for (DiagnosticsPlugin plugin : pluginsMap.values()) {
            if (!plugin.canBeEnabledDynamically()) {
                return true;
            }
        }
        return false;
    }

    private void closeScheduler() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
                logger.fine("Diagnostics scheduler was interrupted while shutting down.", e);
            } finally {
                scheduler = null;
            }
        }

        if (autoOffScheduler != null) {
            // autoOff doesn't require graceful shutdown
            autoOffScheduler.shutdownNow();
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

    public void shutdown() {
        synchronized (lifecycleLock) {
            if (status.get() == SERVICE_SHUTDOWN) {
                return;
            }

            status.set(SERVICE_SHUTDOWN);

            cancelRunningPlugins();
            closeScheduler();
            closeLog();
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

    //created for testing purposes
    ScheduledFuture<?> getAutoOffFuture() {
        return autoOffFuture;
    }

    @SuppressWarnings({"NPathComplexity", "java:S3776", "CyclomaticComplexity", "MethodLength"})
    private void setConfig0(DiagnosticsConfig newConfig) {
        // override config object if properties are set
        Set<String> messages = new HashSet<>();

        this.config = new DiagnosticsConfig();

        if (hazelcastProperties.containsKey(OUTPUT_TYPE)) {
            this.outputType = hazelcastProperties.getEnum(OUTPUT_TYPE, DiagnosticsOutputType.class);
            messages.add(OUTPUT_TYPE.getName() + " = " + outputType);
        } else {
            this.outputType = newConfig.getOutputType();
        }

        if (hazelcastProperties.containsKey(MAX_ROLLED_FILE_SIZE_MB)) {
            this.maxRollingFileSizeMB = hazelcastProperties.getInteger(MAX_ROLLED_FILE_SIZE_MB);
            messages.add(MAX_ROLLED_FILE_SIZE_MB.getName() + " = " + maxRollingFileSizeMB);
        } else {
            this.maxRollingFileSizeMB = newConfig.getMaxRolledFileSizeInMB();
        }

        if (hazelcastProperties.containsKey(MAX_ROLLED_FILE_COUNT)) {
            this.maxRollingFileCount = hazelcastProperties.getInteger(MAX_ROLLED_FILE_COUNT);
            messages.add(MAX_ROLLED_FILE_COUNT.getName() + " = " + maxRollingFileCount);
        } else {
            this.maxRollingFileCount = newConfig.getMaxRolledFileCount();
        }

        if (hazelcastProperties.containsKey(FILENAME_PREFIX)) {
            this.filePrefix = hazelcastProperties.getString(FILENAME_PREFIX);
            messages.add(FILENAME_PREFIX.getName() + " = " + filePrefix);
        } else {
            this.filePrefix = newConfig.getFileNamePrefix();
        }

        if (hazelcastProperties.containsKey(INCLUDE_EPOCH_TIME)) {
            this.includeEpochTime = hazelcastProperties.getBoolean(INCLUDE_EPOCH_TIME);
            messages.add(INCLUDE_EPOCH_TIME.getName() + " = " + includeEpochTime);
        } else {
            this.includeEpochTime = newConfig.isIncludeEpochTime();
        }

        if (hazelcastProperties.containsKey(DIRECTORY)) {
            this.loggingDirectory = new File(hazelcastProperties.getString(DIRECTORY));
            messages.add(DIRECTORY.getName() + " = " + loggingDirectory);
        } else {
            this.loggingDirectory = new File(newConfig.getLogDirectory());
        }

        if (hazelcastProperties.containsKey(ENABLED)) {
            boolean enabled = hazelcastProperties.getBoolean(ENABLED);
            if (enabled) {
                status.set(SERVICE_ENABLED);
            } else {
                status.set(SERVICE_DISABLED);
            }
            messages.add(ENABLED.getName() + " = " + enabled);
        } else {
            status.set(newConfig.isEnabled() ? SERVICE_ENABLED : SERVICE_DISABLED);
        }

        // the config may be overridden by the properties, so we need to set it again
        // these steps should be removed after properties deprecated.
        this.config.setOutputType(outputType);
        this.config.setMaxRolledFileSizeInMB(maxRollingFileSizeMB);
        this.config.setMaxRolledFileCount(maxRollingFileCount);
        this.config.setLogDirectory(loggingDirectory.getAbsolutePath());
        this.config.setFileNamePrefix(filePrefix);
        this.config.setIncludeEpochTime(includeEpochTime);
        this.config.setEnabled(status.get() == SERVICE_ENABLED);
        this.config.getPluginProperties().putAll(newConfig.getPluginProperties());
        this.config.setAutoOffDurationInMinutes(newConfig.getAutoOffDurationInMinutes());
        overridePluginProperties(messages);

        if (isEnabled() && !messages.isEmpty()) {
            StringBuilder sb = new StringBuilder("Diagnostics configs overridden by property: {");
            int i = 0;
            int size = messages.size();
            for (String message : messages) {
                sb.append(message);
                if (i < size - 1) {
                    sb.append(", ");
                }
                i++;
            }
            sb.append("}");
            logger.info(sb.toString());
        }
    }

    /**
     * Schedules the auto off timer if the timer is set to >0 and the service is Enabled. Each scheduling call will cancel
     * the previous one if there is any. The scheduled auto off task will try to disable the diagnostics
     * until interrupted or succeed.
     * <p>
     */
    private void scheduleAutoOff() {
        if (status.get() == SERVICE_SHUTDOWN) {
            return;
        }

        cancelAutoOffFuture();

        if (!(config.getAutoOffDurationInMinutes() > 0 && isEnabled())) {
            return;
        }

        if (autoOffScheduler == null || autoOffScheduler.isShutdown()) {
            autoOffScheduler = Executors.newSingleThreadScheduledExecutor(
                    new DiagnosticSchedulerThreadFactory("DiagnosticsAutoOffThread"));
        }

        setAutoOffFuture0();
    }

    private void cancelAutoOffFuture() {
        if (autoOffFuture != null) {
            autoOffFuture.cancel(true);
            autoOffFuture = null;
            logger.info("Existing auto off future cancelled.");
        }
    }

    /**
     * This method should be called only by the auto off scheduler. It's seperated to reduce the cyclomatic complexity of the
     * scheduleAutoOff method.
     */
    private void setAutoOffFuture0() {
        logger.info(String.format("Diagnostics service is going to be disabled after %d %s.",
                config.getAutoOffDurationInMinutes(), autoOffDurationUnit.name()));

        autoOffFuture = autoOffScheduler.schedule(() -> {
            if (!isEnabled()) {
                logger.fine("Diagnostics service is already disabled. Skipping to schedule the auto off timer");
                return;
            }
            int tryCount = 0;
            // In case of failure, we will try to disable the diagnostics service.
            // This is a safety measure to ensure that the diagnostics is disabled.
            while (!Thread.currentThread().isInterrupted() && isEnabled()) {
                try {
                    DiagnosticsConfig dConfig = new DiagnosticsConfig(config);
                    dConfig.setEnabled(false);
                    setConfig(dConfig);
                    break;
                } catch (Exception e) {
                    tryCount++;
                    logger.warning("Auto off failed to disable diagnostics. Attempt #" + tryCount, e);
                    try {
                        TimeUnit.SECONDS.sleep(AUTO_OFF_BACKOFF_SECONDS);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        logger.fine("Auto off interrupted while sleeping. Exiting...");
                        break;
                    }
                }
            }
        }, config.getAutoOffDurationInMinutes(), autoOffDurationUnit);
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
            if (pluginsFutureMap.containsKey(entry.getValue()) && entry.getValue().canBeEnabledDynamically()) {
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
        staticTasks.set(new DiagnosticsPlugin[0]);
    }

    // override plugin properties from hazelcast properties to diagnostics config if any
    private void overridePluginProperties(Set<String> messages) {
        for (String prop : this.hazelcastProperties.keySet()) {
            if (prop.startsWith(DIAGNOSTIC_PROPERTY_PREFIX)) {
                this.config.getPluginProperties().put(prop, this.hazelcastProperties.get(prop));
                messages.add(prop + " = " + this.hazelcastProperties.get(prop));
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

        private String name;

        DiagnosticSchedulerThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable target) {
            return new Thread(target, createThreadName(hzName, name));
        }
    }
}
