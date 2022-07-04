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

package com.hazelcast.azure;

import com.hazelcast.spi.utils.PortRange;

/**
 * Azure Discovery Strategy configuration that corresponds to the properties passed in the Hazelcast configuration and
 * listed in {@link AzureProperties}.
 */
final class AzureConfig {
    private String tenantId;
    private String clientId;
    private String clientSecret;
    private String subscriptionId;
    private String resourceGroup;
    private String scaleSet;
    private Tag tag;
    private PortRange hzPort;
    private Boolean instanceMetadataAvailable;

    private AzureConfig() {
    }

    static Builder builder() {
        return new Builder();
    }

    String getTenantId() {
        return tenantId;
    }

    String getSubscriptionId() {
        return subscriptionId;
    }

    String getClientId() {
        return clientId;
    }

    String getClientSecret() {
        return clientSecret;
    }

    String getResourceGroup() {
        return resourceGroup;
    }

    String getScaleSet() {
        return scaleSet;
    }

    Tag getTag() {
        return tag;
    }

    PortRange getHzPort() {
        return hzPort;
    }

    public Boolean isInstanceMetadataAvailable() {
        return instanceMetadataAvailable;
    }

    static final class Builder {

        private final AzureConfig config;

        private Builder() {
            this.config = new AzureConfig();
        }

        Builder setTenantId(String tenantId) {
            this.config.tenantId = tenantId;
            return this;
        }

        Builder setSubscriptionId(String subscriptionId) {
            this.config.subscriptionId = subscriptionId;
            return this;
        }

        Builder setClientId(String clientId) {
            this.config.clientId = clientId;
            return this;
        }

        Builder setClientSecret(String clientSecret) {
            this.config.clientSecret = clientSecret;
            return this;
        }

        Builder setResourceGroup(String resourceGroup) {
            this.config.resourceGroup = resourceGroup;
            return this;
        }

        Builder setScaleSet(String scaleSet) {
            this.config.scaleSet = scaleSet;
            return this;
        }

        Builder setTag(Tag tag) {
            this.config.tag = tag;
            return this;
        }

        Builder setHzPort(PortRange hzPort) {
            this.config.hzPort = hzPort;
            return this;
        }

        Builder setInstanceMetadataAvailable(Boolean instanceMetadataAvailable) {
            this.config.instanceMetadataAvailable = instanceMetadataAvailable;
            return this;
        }

        AzureConfig build() {
            return this.config;
        }
    }
}
