/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.gcp;

import java.util.ArrayList;
import java.util.List;

/**
 * GCP Discovery Strategy configuration that corresponds to the properties passed in the Hazelcast configuration and listed in
 * {@link GcpProperties}.
 */
final class GcpConfig {
    private final List<String> projects;
    private final List<String> zones;
    private final String label;
    private final PortRange hzPort;

    private GcpConfig(List<String> projects, List<String> zones, String label, PortRange hzPort) {
        this.projects = projects;
        this.zones = zones;
        this.label = label;
        this.hzPort = hzPort;
    }

    List<String> getProjects() {
        return projects;
    }

    List<String> getZones() {
        return zones;
    }

    String getLabel() {
        return label;
    }

    PortRange getHzPort() {
        return hzPort;
    }

    static Builder builder() {
        return new Builder();
    }

    static final class Builder {
        private List<String> projects = new ArrayList<String>();
        private List<String> zones = new ArrayList<String>();
        private String label;
        private PortRange hzPort;

        Builder setProjects(List<String> projects) {
            this.projects = projects;
            return this;
        }

        Builder setZones(List<String> zones) {
            this.zones = zones;
            return this;
        }

        Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        Builder setHzPort(PortRange hzPort) {
            this.hzPort = hzPort;
            return this;
        }

        GcpConfig build() {
            return new GcpConfig(projects, zones, label, hzPort);
        }
    }
}
