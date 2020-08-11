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

package com.hazelcast.azure;

import java.util.Set;

/**
 * AzureNetworkInterface
 */
final class AzureNetworkInterface {
    private final String privateIp;
    private final String publicIpId;
    private final Set<Tag> tags;

    AzureNetworkInterface(String privateIp, String publicIpId, Set<Tag> tags) {
        this.privateIp = privateIp;
        this.publicIpId = publicIpId;
        this.tags = tags;
    }

    String getPrivateIp() {
        return privateIp;
    }

    String getPublicIpId() {
        return publicIpId;
    }

    boolean hasTag(Tag tag) {
        return this.tags.contains(tag);
    }
}
