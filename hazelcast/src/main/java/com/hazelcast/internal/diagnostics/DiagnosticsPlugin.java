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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin for the {@link Diagnostics}.
 * <p>
 * The plugin will not be called concurrently, unless threads are introduced
 * outside of the {@link Diagnostics}.
 * <p>
 * There is a happens-before relation between {@link #onStart()} and
 * {@link #run(DiagnosticsLogWriter)}, and therefore there is no need to make
 * variables volatile. The source of the happens-before relation is the
 * scheduler.queue inside of the {@link Diagnostics} or the AtomicReference in
 * case of static plugins.
 */
public abstract class DiagnosticsPlugin {

    /**
     * Indicates that a plugin should be run once.
     */
    static final long RUN_ONCE_PERIOD_MS = -1;

    /**
     * Indicates that the plugin is disabled and not going to be scheduled at all to run.
     */
    static final long NOT_SCHEDULED_PERIOD_MS = 0;

    protected final ILogger logger;
    /**
     * Indicates that weather plugin is scheduled to run or not.
     */
    final AtomicBoolean isActive = new AtomicBoolean(false);
    private final Map<String, String> properties = new ConcurrentHashMap<>();


    protected DiagnosticsPlugin(ILogger logger) {
        this.logger = logger;
    }

    protected void setActive() {
        isActive.set(true);
    }

    protected void setInactive() {
        isActive.set(false);
    }

    /**
     * Indicates that plugin is scheduled to run.
     *
     * @return true if the plugin is scheduled to run, false otherwise.
     */
    public boolean isActive() {
        return isActive.get();
    }

    /**
     * Returns the period of executing the monitor in millis.
     * <p>
     * If a monitor is disabled, 0 is returned.
     * <p>
     * If a monitor should run only once, a negative value is returned. This is
     * useful for 'static' monitors like the {@link SystemPropertiesPlugin}
     * that run at the beginning of a log file but their contents will not
     * change.
     *
     * @return the period in millis.
     */
    public abstract long getPeriodMillis();

    public void onStart() {
        setActive();
    }

    public void onShutdown() {
        setInactive();
    }

    /**
     * Indicates if the plugin can be enabled at runtime.
     *
     * @return true if the plugin can be enabled at runtime, false otherwise.
     */
    public boolean canBeEnabledDynamically() {
        return true;
    }

    void setProperties(Map<String, String> props) {
        setProperties(props, true);
    }

    private void setProperties(Map<String, String> props, boolean refresh) {
        if (!isActive() && !this.properties.equals(props)) {
            this.properties.clear();
            this.properties.putAll(props);
            if (refresh) {
                readProperties();
            }
        }
    }

    /**
     * Reads the properties from the diagnostics config.
     * <p>
     * This method is called when the plugin is initialized and when the
     * properties are set. The implementer should read its properties from the config.
     * </p>
     */
    abstract void readProperties();

    /**
     * Overrides the property with the value from the diagnostics config if it is set.
     */
    HazelcastProperty overrideProperty(HazelcastProperty property) {
        String value = properties.get(property.getName());

        if (value == null) {
            return property;
        }

        property.setSystemProperty(value);
        return property;
    }

    public abstract void run(DiagnosticsLogWriter writer);
}
