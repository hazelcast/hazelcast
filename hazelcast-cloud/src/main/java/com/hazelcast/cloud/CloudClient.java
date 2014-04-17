/*
 * Copyright 2014 Hazelcast, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.cloud;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import static com.google.common.base.Strings.emptyToNull;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import com.hazelcast.config.CloudConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import static org.jclouds.compute.predicates.NodePredicates.TERMINATED;
import static org.jclouds.compute.predicates.NodePredicates.inGroup;
import static org.jclouds.compute.predicates.NodePredicates.locationId;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;

/**
 * Simple cloud provider client, powered by jclouds, that retrieves private IP
 * addresses from nodes queried with pre-configured predicates.
 */
public class CloudClient {

    private CloudConfig config;

    public CloudClient(CloudConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("CloudConfig is required!");
        }
        if (config.getProvider() == null) {
            if (!config.getProvider().contains("aws-ec2")
                    && !config.getProvider().contains("google-compute-engine")) {
                throw new IllegalArgumentException("A valid cloud provider is required!");
            }
        }
        if (config.getAccessKey() == null) {
            throw new IllegalArgumentException(
                    "Cloud provider access key is required!");
        }
        if (config.getSecretKey() == null) {
            throw new IllegalArgumentException(
                    "Cloud provider secret key is required!");
        }
        this.config = config;
    }

    /**
     * Queries cloud provider for private IP addresses from hosts that respect
     * the criteria defined in the configuration.
     *
     * @return a list of private IP addresses to be used in TCP/IP join cluster.
     * @throws Exception
     */
    public List<String> getPrivateIpAddresses() {
        // instantiate compute service client
        ComputeService computeService = initComputeService(config.getAccessKey(),
                config.getSecretKey());

        // build predicate based on configuration
        Predicate filter = Predicates.<NodeMetadata>not(TERMINATED);
        // is region defined?
        if (config.getRegion() != null && !config.getRegion().isEmpty()) {
            filter = Predicates.<NodeMetadata>and(filter,
                    locationId(config.getRegion()));
        }
        // is groupName defined?
        if (config.getGroupName() != null && !config.getGroupName().isEmpty()) {
            filter = Predicates.<NodeMetadata>and(filter,
                    inGroup(config.getGroupName()));
        }
        // is tag defined?
        if (config.getTagKey() != null && !config.getTagKey().isEmpty()) {
            filter = Predicates.<NodeMetadata>and(filter,
                    hasTagKeyWithTagValue(config.getTagKey(), config.getTagValue()));
        }

        // compile private IP addresses list
        List<String> privateIps = new ArrayList<String>();
        Set<? extends ComputeMetadata> nodes = computeService.listNodesDetailsMatching(filter);
        for (ComputeMetadata node : nodes) {
            for (String privateIp : ((NodeMetadata) node).getPrivateAddresses()) {
                privateIps.add(privateIp);
            }
        }

        return privateIps;
    }

    /**
     * Initializes compute service.
     *
     * @param accessKey the cloud provider access key
     * @param secretKey the cloud provider secret key
     * @return the initialized compute service.
     */
    private ComputeService initComputeService(final String accessKey,
            final String secretKey) {
        Iterable<Module> modules = ImmutableSet
                .<Module>of(new EnterpriseConfigurationModule());
        ContextBuilder builder = ContextBuilder.newBuilder(config.getProvider())
                .credentials(accessKey, secretKey).modules(modules);

        return builder.buildView(ComputeServiceContext.class).getComputeService();
    }

    /**
     * Builds predicate for filtering query to provider based on user-metadata.
     *
     * @param tagKey
     * @param tagValue
     * @return the built predicate.
     */
    private static Predicate<NodeMetadata> hasTagKeyWithTagValue(
            final String tagKey, final String tagValue) {
        checkNotNull(emptyToNull(tagKey), "tag key must be defined");
        checkNotNull(emptyToNull(tagValue), "tag value must be defined");
        return new Predicate<NodeMetadata>() {
            @Override
            public boolean apply(NodeMetadata nodeMetadata) {
                return nodeMetadata.getUserMetadata().containsKey(tagKey)
                        && nodeMetadata.getUserMetadata().get(tagKey).equals(tagValue);
            }

            @Override
            public String toString() {
                return "hasTagKeyWithTagValue(" + tagKey + ", " + tagValue + ")";
            }
        };
    }

}
