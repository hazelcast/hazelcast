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

package com.hazelcast.config;

import com.hazelcast.internal.util.EmptyStatement;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Configures tracking of a running Hazelcast instance. For now, this is
 * limited to writing information about the Hazelcast instance to a file
 * while the instance is starting.
 * <p>
 * The file is overwritten on every start of the Hazelcast instance and if
 * multiple instance share the same file system, every instance will
 * overwrite the tracking file of a previously started instance.
 * <p>
 * If this instance is unable to write the file, the exception is logged and
 * the instance is allowed to start.
 *
 * @since 4.1
 */
public class InstanceTrackingConfig {

    /**
     * Default file to which the instance metadata will be written.
     */
    public static final Path DEFAULT_FILE = Paths.get(System.getProperty("java.io.tmpdir"), "Hazelcast.process");

    /**
     * Namespace for the placeholders in the file name and format pattern to
     * distinguish between different types of placeholders.
     */
    public static final String PLACEHOLDER_NAMESPACE = "HZ_INSTANCE_TRACKING";

    boolean isEnabledSet;

    /**
     * Setting whether instance tracking should be enabled or not.
     * It depends on whether the instance is an OEM/NLC build or not.
     */
    private boolean enabled = isOEMBuild();

    private String fileName;
    private String formatPattern;


    public InstanceTrackingConfig() {
        super();
    }

    public InstanceTrackingConfig(InstanceTrackingConfig other) {
        this.enabled = other.enabled;
        this.fileName = other.fileName;
        this.formatPattern = other.formatPattern;
    }

    /**
     * Returns {@code true} if instance tracking is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables instance tracking.
     *
     * @param enabled {@code true} if instance tracking should be enabled
     * @return this configuration
     */
    public InstanceTrackingConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        this.isEnabledSet = true;
        return this;
    }

    /**
     * Returns the name of the file which will contain the tracking metadata. If
     * {@code null}, {@link InstanceTrackingConfig#DEFAULT_FILE} is used instead.
     * <p>
     * The filename can contain placeholders that will be resolved in the same way
     * as placeholders for the format pattern (see {@link #setFormatPattern(String)}).
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Sets the name of the file which will contain the tracking metadata. If set
     * to {@code null}, {@link InstanceTrackingConfig#DEFAULT_FILE} is used instead.
     * <p>
     * The filename can contain placeholders that will be resolved in the same way
     * as placeholders for the format pattern (see {@link #setFormatPattern(String)}).
     *
     * @param fileName the name of the file to contain the tracking metadata
     * @return this configuration
     */
    public InstanceTrackingConfig setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    /**
     * Returns the pattern used to render the contents of the instance tracking file.
     * It may contain placeholders for properties listed in the
     * {@link InstanceTrackingProperties} enum. The placeholders are defined by
     * a $HZ_INSTANCE_TRACKING&#123; prefix and followed by &#125;. For instance, a placeholder for
     * the {@link InstanceTrackingProperties#START_TIMESTAMP}
     * would be $HZ_INSTANCE_TRACKING&#123;start_timestamp&#125;.
     * <p>
     * The placeholders are resolved in a fail-safe manner. Any incorrect syntax
     * is ignored and only the known properties are resolved, placeholders for
     * any parameters which do not have defined values will be ignored. This also
     * means that if there is a missing closing bracket in one of the placeholders,
     * the property name will be resolved as anything from the opening bracket
     * to the next closing bracket, which might contain additional opening brackets.
     * <p>
     * If set to {@code null}, a JSON formatted output will be used.
     *
     * @return the pattern for the instance tracking file
     * @see InstanceTrackingProperties
     */
    public String getFormatPattern() {
        return formatPattern;
    }

    /**
     * Sets the pattern used to render the contents of the instance tracking file.
     * It may contain placeholders for properties listed in the
     * {@link InstanceTrackingProperties} enum. The placeholders are defined by
     * a $HZ_INSTANCE_TRACKING&#123; prefix and followed by &#125;. For instance, a placeholder for
     * the {@link InstanceTrackingProperties#START_TIMESTAMP}
     * would be $HZ_INSTANCE_TRACKING&#123;start_timestamp&#125;.
     * <p>
     * The placeholders are resolved in a fail-safe manner. Any incorrect syntax
     * is ignored and only the known properties are resolved, placeholders for
     * any parameters which do not have defined values will be ignored. This also
     * means that if there is a missing closing bracket in one of the placeholders,
     * the property name will be resolved as anything from the opening bracket
     * to the next closing bracket, which might contain additional opening brackets.
     * <p>
     * If set to {@code null}, a JSON formatted output will be used.
     *
     * @param formatPattern the pattern for the instance tracking file
     * @return this configuration
     */
    public InstanceTrackingConfig setFormatPattern(String formatPattern) {
        this.formatPattern = formatPattern;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InstanceTrackingConfig that = (InstanceTrackingConfig) o;
        return enabled == that.enabled
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(formatPattern, that.formatPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, fileName, formatPattern);
    }

    @Override
    public String toString() {
        return "InstanceTrackingConfig{"
                + "enabled=" + enabled
                + ", fileName='" + fileName + '\''
                + ", formatPattern='" + formatPattern + '\''
                + '}';
    }

    /**
     * Enumeration of instance properties provided to the format pattern for
     * output.
     */
    public enum InstanceTrackingProperties {
        /**
         * The instance product name, e.g. "Hazelcast" or "Hazelcast Enterprise".
         *
         * @see InstanceProductName
         */
        PRODUCT("product"),
        /**
         * The instance version.
         */
        VERSION("version"),
        /**
         * The instance mode, e.g. "server", "embedded" or "client".
         *
         * @see InstanceMode
         */
        MODE("mode"),
        /**
         * The timestamp of when the instance was started as returned by
         * {@link System#currentTimeMillis()}.
         */
        START_TIMESTAMP("start_timestamp"),
        /**
         * If this instance is using a license or not. The value {@code 0} signifies
         * that there is no license set and the value {@code 1} signifies that a
         * license is in use.
         */
        LICENSED("licensed"),
        /**
         * Attempts to get the process ID value. The algorithm does not guarantee to
         * get the process ID on all JVMs and operating systems so please test before
         * use.
         * In case we are unable to get the PID, the value will be {@code -1}.
         */
        PID("pid");

        private final String propertyName;

        InstanceTrackingProperties(String propertyName) {
            this.propertyName = propertyName;
        }

        /**
         * Returns the property name which can be used in placeholders to be resolved
         * to an actual property value.
         */
        public String getPropertyName() {
            return propertyName;
        }
    }

    /**
     * Product name for the Hazelcast instance
     */
    public enum InstanceProductName {
        /**
         * Hazelcast open-source
         */
        HAZELCAST("Hazelcast"),
        /**
         * Hazelcast Enterprise
         */
        HAZELCAST_EE("Hazelcast Enterprise"),
        /**
         * Hazelcast java open-source client
         */
        HAZELCAST_CLIENT("Hazelcast Client"),
        /**
         * Hazelcast java enterprise client
         */
        HAZELCAST_CLIENT_EE("Hazelcast Client Enterprise"),
        /**
         * Hazelcast Jet open-source
         */
        HAZELCAST_JET("Hazelcast Jet"),
        /**
         * Hazelcast Jet enterprise
         */
        HAZELCAST_JET_EE("Hazelcast Jet Enterprise");

        private final String productName;

        InstanceProductName(String productName) {
            this.productName = productName;
        }

        /**
         * Returns the string representation of the instance product name
         */
        public String getProductName() {
            return productName;
        }
    }

    /**
     * The mode in which this instance is running.
     */
    public enum InstanceMode {
        /**
         * This instance was started using the {@code start.sh} or {@code start.bat}
         * scripts.
         */
        SERVER("server"),
        /**
         * This instance is a Hazelcast client instance.
         */
        CLIENT("client"),
        /**
         * This instance is embedded in another Java program.
         */
        EMBEDDED("embedded");

        private final String modeName;

        InstanceMode(String modeName) {
            this.modeName = modeName;
        }

        /**
         * Returns the string representation of the instance mode name.
         */
        public String getModeName() {
            return modeName;
        }
    }

    /**
     * Returns whether this build is an OEM/NLC build or not.
     */
    private static boolean isOEMBuild() {
        try {
            Class<?> helper = Class.forName("com.hazelcast.license.util.LicenseHelper");
            Method getLicense = helper.getMethod("getBuiltInLicense");
            // enabled for OEM/NLC build
            return getLicense.invoke(null) != null;
        } catch (ClassNotFoundException | NoSuchMethodException
                | IllegalAccessException | InvocationTargetException e) {
            // running OS, instance tracking is disabled by default
            EmptyStatement.ignore(e);
        }
        return false;
    }
}
