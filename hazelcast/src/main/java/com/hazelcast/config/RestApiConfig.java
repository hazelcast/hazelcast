/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * This class allows to control which parts of Hazelcast REST API will be enabled. There are 2 levels of control:
 * <ul>
 * <li>overall REST access (enabled by default);</li>
 * <li>access to REST endpoint groups (see {@link RestEndpointGroup}).</li>
 * </ul>
 */
public class RestApiConfig {

    private boolean enabled;

    private final Set<RestEndpointGroup> enabledGroups = Collections.synchronizedSet(EnumSet.noneOf(RestEndpointGroup.class));

    public RestApiConfig() {
        for (RestEndpointGroup eg : RestEndpointGroup.values()) {
            if (eg.isEnabledByDefault()) {
                enabledGroups.add(eg);
            }
        }
    }

    /**
     * Enables all REST endpoint groups.
     */
    public RestApiConfig enableAllGroups() {
        return enableGroups(RestEndpointGroup.values());
    }

    /**
     * Enables provided REST endpoint groups. It doesn't replace already enabled groups.
     */
    public RestApiConfig enableGroups(RestEndpointGroup... endpointGroups) {
        if (endpointGroups != null) {
            enabledGroups.addAll(Arrays.asList(endpointGroups));
        }
        return this;
    }

    /**
     * Disables all REST endpoint groups.
     */
    public RestApiConfig disableAllGroups() {
        enabledGroups.clear();
        return this;
    }

    /**
     * Disables provided REST endpoint groups.
     */
    public RestApiConfig disableGroups(RestEndpointGroup... endpointGroups) {
        if (endpointGroups != null) {
            enabledGroups.removeAll(Arrays.asList(endpointGroups));
        }
        return this;
    }

    /**
     * Checks if REST API access is enabled. This flag controls access to all REST resources on a Hazelcast member. Once the
     * REST API is enabled you can control access to REST endpoints by enabling/disabling enpoint groups.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Return true if the REST API is enabled and at least one REST endpoint group is allowed.
     */
    public boolean isEnabledAndNotEmpty() {
        return enabled && !enabledGroups.isEmpty();
    }

    /**
     * Enables or disables the REST API on the member.
     */
    public RestApiConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns a not-{@code null} set of enabled REST endpoint groups.
     */
    public Set<RestEndpointGroup> getEnabledGroups() {
        return new HashSet<RestEndpointGroup>(enabledGroups);
    }

    /**
     * Checks if given REST endpoint group is enabled. It can return {@code true} even if the REST API itself is disabled.
     */
    public boolean isGroupEnabled(RestEndpointGroup group) {
        return enabledGroups.contains(group);
    }

    public RestApiConfig setEnabledGroups(Collection<RestEndpointGroup> groups) {
        enabledGroups.clear();
        if (groups != null) {
            enabledGroups.addAll(groups);
        }
        return this;
    }

    @Override
    public String toString() {
        return "RestApiConfig{enabled=" + enabled + ", enabledGroups=" + enabledGroups + "}";
    }
}
