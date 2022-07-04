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

package com.hazelcast.internal.util;

import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static com.hazelcast.internal.util.StringUtil.resolvePlaceholders;

/**
 * Helper class for instance tracking.
 *
 * @see InstanceTrackingConfig
 */
public final class InstanceTrackingUtil {

    /**
     * System property that is set with the instance tracking full file path.
     * This property then can be audited via jcmd.
     */
    public static final String HAZELCAST_CONFIG_INSTANCE_TRACKING_FILE = "hazelcast.config.instance.tracking.file";

    private InstanceTrackingUtil() {
    }

    /**
     * Writes the instance tracking metadata to a file provided by the
     * {@code fileName}. Both the file name and the format pattern may contain
     * placeholders for values provided by {@code placeholderValues}. If the
     * {@code fileName} is {@code null}, the {@link InstanceTrackingConfig#DEFAULT_FILE}
     * is used instead.
     * If the {@code formatPattern} is {@code null}, a JSON formatted output is
     * used instead.
     *
     * @param fileName          the file name to which to write to
     * @param formatPattern     the pattern for the contents of the tracking file
     * @param placeholderValues the values for the placeholders
     * @param logger            a logger for information on instance tracking file write progress
     */
    public static void writeInstanceTrackingFile(@Nullable String fileName,
                                                 @Nullable String formatPattern,
                                                 @Nonnull Map<String, Object> placeholderValues,
                                                 @Nonnull ILogger logger) {
        try {
            String trackingFileContents = getInstanceTrackingContent(formatPattern, placeholderValues);
            Path file = getInstanceTrackingFilePath(fileName, placeholderValues);

            // Set the instance tracking file path to a system property so it can be audited
            System.setProperty(HAZELCAST_CONFIG_INSTANCE_TRACKING_FILE, file.toString());

            logger.fine("Writing instance tracking information to " + file);
            Files.write(file, trackingFileContents.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.warning("Failed to write instance tracking information", e);
        }
    }

    /**
     * Returns the path to the instance tracking file with resolved placeholders
     * in the file name.
     */
    private static Path getInstanceTrackingFilePath(String fileName, Map<String, Object> trackingFileProperties) {
        if (fileName == null) {
            return InstanceTrackingConfig.DEFAULT_FILE;
        }
        return Paths.get(resolvePlaceholders(fileName, InstanceTrackingConfig.PLACEHOLDER_NAMESPACE, trackingFileProperties));
    }

    /**
     * Returns the string containing instance tracking metadata with resolved
     * placeholders for supported properties.
     *
     * @param formatPattern  the pattern which should be used to format the string
     * @param propertyValues property values which will be used to resolve the placeholders
     * @return the string containing instance tracking metadata
     * @see com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties
     */
    private static String getInstanceTrackingContent(String formatPattern, Map<String, Object> propertyValues) {
        if (formatPattern == null) {
            JsonObject jsonObject = new JsonObject();
            propertyValues.forEach((prop, value) -> {
                if (value instanceof Boolean) {
                    jsonObject.add(prop, (boolean) value);
                } else if (value instanceof Integer) {
                    jsonObject.add(prop, (int) value);
                } else if (value instanceof Long) {
                    jsonObject.add(prop, (long) value);
                } else {
                    jsonObject.add(prop, value.toString());
                }
            });
            return jsonObject.toString();
        } else {
            return resolvePlaceholders(formatPattern, InstanceTrackingConfig.PLACEHOLDER_NAMESPACE, propertyValues);
        }
    }
}
