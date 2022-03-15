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

package com.hazelcast.internal.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.StringUtil;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 * Utility class for handling declarative configuration files.
 */
public final class DeclarativeConfigUtil {

    /**
     * System property used for defining the configuring file used for
     * member configuration.
     */
    public static final String SYSPROP_MEMBER_CONFIG = "hazelcast.config";

    /**
     * System property used for defining the configuring file used for
     * client configuration.
     */
    public static final String SYSPROP_CLIENT_CONFIG = "hazelcast.client.config";

    /**
     * System property used for defining the configuring file used for
     * client failover configuration.
     */
    public static final String SYSPROP_CLIENT_FAILOVER_CONFIG = "hazelcast.client.failover.config";

    /**
     * Array of accepted suffixes for XML configuration files.
     */
    public static final Collection<String> XML_ACCEPTED_SUFFIXES = singletonList("xml");

    /**
     * List of accepted suffixes for XML configuration files.
     *
     * @see #XML_ACCEPTED_SUFFIXES
     */
    public static final String XML_ACCEPTED_SUFFIXES_STRING = Arrays.toString(XML_ACCEPTED_SUFFIXES.toArray());

    /**
     * Array of accepted suffixes for YAML configuration files.
     */
    public static final Collection<String> YAML_ACCEPTED_SUFFIXES = unmodifiableList(asList("yaml", "yml"));

    /**
     * List of accepted suffixes for YAML configuration files.
     *
     * @see #YAML_ACCEPTED_SUFFIXES
     */
    public static final String YAML_ACCEPTED_SUFFIXES_STRING = Arrays.toString(YAML_ACCEPTED_SUFFIXES.toArray());

    /**
     * Array of the suffixes accepted in Hazelcast configuration files.
     */
    public static final Collection<String> ALL_ACCEPTED_SUFFIXES = unmodifiableList(asList("xml", "yaml", "yml"));

    /**
     * The list of the suffixes accepted in Hazelcast configuration files.
     *
     * @see #ALL_ACCEPTED_SUFFIXES
     */
    public static final String ALL_ACCEPTED_SUFFIXES_STRING = Arrays.toString(ALL_ACCEPTED_SUFFIXES.toArray());

    private DeclarativeConfigUtil() {
    }

    /**
     * Validates if the config file referenced in {@code propertyKey}
     * has an accepted suffix. If the system property is not set, the
     * validation passes without throwing exception.
     *
     * @param propertyKey The name of the system property to validate
     * @throws HazelcastException If the suffix of the config file name
     *                            is not in the accepted suffix list
     */
    public static void validateSuffixInSystemProperty(String propertyKey) {
        String configSystemProperty = System.getProperty(propertyKey);
        if (configSystemProperty == null) {
            return;
        }

        if (!isAcceptedSuffixConfigured(configSystemProperty, ALL_ACCEPTED_SUFFIXES)) {
            throwUnacceptedSuffixInSystemProperty(propertyKey, configSystemProperty, ALL_ACCEPTED_SUFFIXES);
        }
    }

    /**
     * Throws {@link HazelcastException} unconditionally referring to that
     * the configuration file referenced in the system property
     * {@code propertyKey} has a suffix not in the accepted suffix list
     * defined in {@code acceptedSuffixes}.
     *
     * @param propertyKey          The name of the system property key
     *                             holding the reference to the
     *                             configuration file
     * @param configResource       The value of the system property
     * @param acceptedSuffixes     The list of the accepted suffixes
     * @throws HazelcastException Thrown unconditionally with a message
     *                            referring to the unaccepted suffix of
     *                            the file referenced by {@code propertyKey}
     */
    public static void throwUnacceptedSuffixInSystemProperty(String propertyKey, String configResource,
                                                      Collection<String> acceptedSuffixes) {

        String message = String.format("The suffix of the resource \'%s\' referenced in \'%s\' is not in the list of accepted "
                + "suffixes: \'%s\'", configResource, propertyKey, Arrays.toString(acceptedSuffixes.toArray()));
        throw new HazelcastException(message);
    }

    /**
     * Checks if the suffix of the passed {@code configFile} is in the
     * accepted suffixes list passed in {@code acceptedSuffixes}.
     *
     * @param configFile       The configuration file to check
     * @param acceptedSuffixes The list of the accepted suffixes
     * @return {@code true} if the suffix of the configuration file is in
     * the accepted list, {@code false} otherwise
     */
    public static boolean isAcceptedSuffixConfigured(String configFile, Collection<String> acceptedSuffixes) {
        String configFileLower = StringUtil.lowerCaseInternal(configFile);
        int lastDotIndex = configFileLower.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return false;
        }

        String configFileSuffix = configFileLower.substring(lastDotIndex + 1);
        return acceptedSuffixes.contains(configFileSuffix);
    }
}
