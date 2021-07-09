/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.gcp;

import java.util.ArrayList;
import java.util.List;

/**
 * GCP Discovery Strategy configuration that corresponds to the properties passed in the Hazelcast configuration and listed in
 * {@link GcpProperties}.
 */
final class GcpConfig {
    private final String privateKeyPath;
    private final List<String> projects;
    private final List<String> zones;
    private final Label label;
    private final PortRange hzPort;
    private final String region;

    private GcpConfig(String privateKeyPath, List<String> projects, List<String> zones,
                      Label label, PortRange hzPort, String region) {
        this.privateKeyPath = privateKeyPath;
        this.projects = projects;
        this.zones = zones;
        this.label = label;
        this.hzPort = hzPort;
        this.region = region;
    }

    String getPrivateKeyPath() {
        return privateKeyPath;
    }

    List<String> getProjects() {
        return projects;
    }

    List<String> getZones() {
        return zones;
    }

    Label getLabel() {
        return label;
    }

    PortRange getHzPort() {
        return hzPort;
    }

    String getRegion() {
        return region;
    }

    static Builder builder() {
        return new Builder();
    }

    static final class Builder {
        private String privateKeyPath;
        private List<String> projects = new ArrayList<String>();
        private List<String> zones = new ArrayList<String>();
        private Label label;
        private PortRange hzPort;
        private String region;

        Builder setPrivateKeyPath(String privateKeyPath) {
            this.privateKeyPath = privateKeyPath;
            return this;
        }

        Builder setProjects(List<String> projects) {
            this.projects = projects;
            return this;
        }

        Builder setZones(List<String> zones) {
            this.zones = zones;
            return this;
        }

        Builder setLabel(Label label) {
            this.label = label;
            return this;
        }

        Builder setHzPort(PortRange hzPort) {
            this.hzPort = hzPort;
            return this;
        }

        Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        GcpConfig build() {
            return new GcpConfig(privateKeyPath, projects, zones, label, hzPort, region);
        }
    }
}
