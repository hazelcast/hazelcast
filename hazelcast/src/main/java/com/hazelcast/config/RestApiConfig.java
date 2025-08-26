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

package com.hazelcast.config;

import com.hazelcast.config.rest.RestConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.config.RestEndpointGroup.getAllEndpointGroups;

/**
 * This class allows controlling which parts of Hazelcast REST API will be enabled. There are 2 levels of control:
 * <ul>
 * <li>overall REST access (enabled by default);</li>
 * <li>access to REST endpoint groups (see {@link RestEndpointGroup}).</li>
 * </ul>
 *
 * @deprecated use RestConfig instead. Will be removed at 6.0.
 * @see RestConfig
 */
@Deprecated(since = "5.5", forRemoval = true)
public class RestApiConfig {

    private boolean enabled;

    private final Set<Integer> enabledGroupCodes = Collections.synchronizedSet(new HashSet<>());

    public RestApiConfig() {
        for (RestEndpointGroup eg : getAllEndpointGroups()) {
            if (eg.isEnabledByDefault()) {
                enabledGroupCodes.add(eg.getCode());
            }
        }
    }

    /**
     * Enables all REST endpoint groups.
     */
    public RestApiConfig enableAllGroups() {
        return enableGroups(getAllEndpointGroups().toArray(new RestEndpointGroup[0]));
    }

    /**
     * Enables provided REST endpoint groups. It doesn't replace already enabled groups.
     */
    public RestApiConfig enableGroups(RestEndpointGroup... endpointGroups) {
        if (endpointGroups != null) {
            enabledGroupCodes.addAll(Arrays.stream(endpointGroups).map(RestEndpointGroup::getCode)
                    .collect(Collectors.toSet()));
        }
        return this;
    }

    /**
     * Disables all REST endpoint groups.
     */
    public RestApiConfig disableAllGroups() {
        enabledGroupCodes.clear();
        return this;
    }

    /**
     * Disables provided REST endpoint groups.
     */
    public RestApiConfig disableGroups(RestEndpointGroup... endpointGroups) {
        if (endpointGroups != null) {
            Arrays.stream(endpointGroups).map(RestEndpointGroup::getCode).forEach(enabledGroupCodes::remove);
        }
        return this;
    }

    /**
     * Checks if REST API access is enabled. This flag controls access to all REST resources on a Hazelcast member. Once the
     * REST API is enabled you can control access to REST endpoints by enabling/disabling endpoint groups.
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
        return enabled && !enabledGroupCodes.isEmpty();
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
        return enabledGroupCodes.stream().map(RestEndpointGroup::getRestEndpointGroup).collect(Collectors.toSet());
    }

    /**
     * Checks if given REST endpoint group is enabled. It can return {@code true} even if the REST API itself is disabled.
     */
    public boolean isGroupEnabled(RestEndpointGroup group) {
        return enabledGroupCodes.contains(group.getCode());
    }

    public RestApiConfig setEnabledGroups(Collection<RestEndpointGroup> groups) {
        enabledGroupCodes.clear();
        if (groups != null) {
            enabledGroupCodes.addAll(groups.stream().map(RestEndpointGroup::getCode).collect(Collectors.toSet()));
        }
        return this;
    }

    @Override
    public String toString() {
        return "RestApiConfig{enabled=" + enabled + ", enabledGroups=" + getEnabledGroups() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RestApiConfig that = (RestApiConfig) o;
        return enabled == that.enabled && Objects.equals(enabledGroupCodes, that.enabledGroupCodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, enabledGroupCodes);
    }
}
