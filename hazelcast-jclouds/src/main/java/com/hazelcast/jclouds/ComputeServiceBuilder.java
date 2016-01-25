/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jclouds;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.io.Files;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationScope;
import org.jclouds.googlecloud.GoogleCredentialsFromJson;
import org.jclouds.location.reference.LocationConstants;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * ComputeServiceBuilder is the responsible class for building jclouds compute service provider.
 * Also parses config and applies necessary filters on cluster nodes.
 */
public class ComputeServiceBuilder {

    private static final String GOOGLE_COMPUTE_ENGINE = "google-compute-engine";
    private static final String AWS_EC2 = "aws-ec2";
    private static final String JCLOUD_CONNECTION_TIMEOUT = "10000";
    private static final ILogger LOGGER = Logger.getLogger(ComputeServiceBuilder.class);

    private final Map<String, Comparable> properties;
    private Set<String> regionsSet = new LinkedHashSet<String>();
    private Set<String> zonesSet = new LinkedHashSet<String>();
    private List<AbstractMap.SimpleImmutableEntry> tagPairs = new ArrayList<AbstractMap.SimpleImmutableEntry>();
    private Predicate<ComputeMetadata> nodesFilter;
    private ComputeService computeService;

    /**
     * Instantiates a new Compute service builder.
     *
     * @param properties the properties
     */
    ComputeServiceBuilder(Map<String, Comparable> properties) {
        checkNotNull(properties, "Props cannot be null");
        this.properties = properties;
    }

    public Map<String, Comparable> getProperties() {
        return properties;
    }

    public Set<String> getRegionsSet() {
        return regionsSet;
    }

    public Set<String> getZonesSet() {
        return zonesSet;
    }

    public List<AbstractMap.SimpleImmutableEntry> getTagPairs() {
        return tagPairs;
    }

    /**
     * Injects already built ComputeService
     * @param computeService
     */
    public void setComputeService(ComputeService computeService) {
        this.computeService = computeService;
    }

    /**
     * Gets filtered nodes.
     *
     * @return the filtered nodes
     */
    public  Iterable<? extends NodeMetadata> getFilteredNodes() {
        final String group = getOrNull(JCloudsProperties.GROUP);
        Set<? extends NodeMetadata> result = computeService.listNodesDetailsMatching(nodesFilter);
        Iterable<? extends NodeMetadata> filteredResult = new HashSet<NodeMetadata>();
        for (NodeMetadata metadata : result) {
            if (group != null && !group.equals(metadata.getGroup())) {
                continue;
            }
            if (!isNodeInsideZones(metadata) || !isNodeInsideRegions(metadata)) {
                continue;
            }
            ((HashSet<NodeMetadata>) filteredResult).add(metadata);
        }
        return filteredResult;
    }

    public int getServicePort() {
        return getOrDefault(JCloudsProperties.HZ_PORT, NetworkConfig.DEFAULT_PORT);
    }

    public boolean isNodeInsideZones(NodeMetadata metadata) {
        Location location = metadata.getLocation();
        while (location != null) {
            String id = location.getId();
            if (location.getScope().equals(LocationScope.ZONE)) {
                if (id != null && !zonesSet.isEmpty() && !zonesSet.contains(id)) {
                    return false;
                }
            }
            location = location.getParent();
        }
        return true;
    }
    public boolean isNodeInsideRegions(NodeMetadata metadata) {
        Location location = metadata.getLocation();
        while (location != null) {
            String id = location.getId();
            if (location.getScope().equals(LocationScope.REGION)) {
                if (id != null && !regionsSet.isEmpty() && !regionsSet.contains(id)) {
                    return false;
                }
            }
            location = location.getParent();
        }
        return true;
    }

    /**
     */
    public void destroy() {
        if (computeService != null) {
            this.computeService.getContext().close();
        }
    }

    /**
     * Build compute service.
     *
     * @return the compute service
     */
    ComputeService build() {
        final String cloudProvider = getOrNull(JCloudsProperties.PROVIDER);
        final String identity = getOrNull(JCloudsProperties.IDENTITY);
        String credential = getOrNull(JCloudsProperties.CREDENTIAL);
        final String credentialPath = getOrNull(JCloudsProperties.CREDENTIAL_PATH);
        isNotNull(cloudProvider, "Cloud Provider");

        if (credential != null && credentialPath != null) {
            throw new UnsupportedOperationException("Both credential and credentialPath are set. Use only one method.");
        }
        if (credentialPath != null) {
            credential = getCredentialFromFile(credential, credentialPath);
        }

        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest("Using CLOUD_PROVIDER: " + cloudProvider);
        }

        final String roleName = getOrNull(JCloudsProperties.ROLE_NAME);
        ContextBuilder contextBuilder = newContextBuilder(cloudProvider, identity, credential, roleName);

        Properties jcloudsProperties = buildRegionZonesConfig();
        buildTagConfig();
        buildNodeFilter();

        computeService = contextBuilder.overrides(jcloudsProperties)
                .buildView(ComputeServiceContext.class)
                .getComputeService();

        return computeService;
    }


    public Properties buildRegionZonesConfig() {
        final String regions = getOrNull(JCloudsProperties.REGIONS);
        final String zones = getOrNull(JCloudsProperties.ZONES);

        Properties jcloudsProperties = newOverrideProperties();
        if (regions != null) {
            List<String> regionList = Arrays.asList(regions.split(","));
            for (String region : regionList) {
                regionsSet.add(region);
            }
            jcloudsProperties.setProperty(LocationConstants.PROPERTY_REGIONS, regions);
        }
        if (zones != null) {
            List<String> zoneList = Arrays.asList(zones.split(","));
            for (String zone : zoneList) {
                zonesSet.add(zone);
            }
            jcloudsProperties.setProperty(LocationConstants.PROPERTY_ZONES, zones);
        }
        return jcloudsProperties;
    }

    public void buildTagConfig() {
        final String tagKeys = getOrNull(JCloudsProperties.TAG_KEYS);
        final String tagValues = getOrNull(JCloudsProperties.TAG_VALUES);
        if (tagKeys != null && tagValues != null) {
            List<String> keysList = Arrays.asList(tagKeys.split(","));
            List<String> valueList = Arrays.asList(tagValues.split(","));
            if (keysList.size() != valueList.size()) {
                throw new InvalidConfigurationException("Tags keys and value count does not match.");
            }
            for (int i = 0; i < keysList.size(); i++) {
                tagPairs.add(new AbstractMap.SimpleImmutableEntry(keysList.get(i), valueList.get(i)));
            }
        }
    }

    public Predicate<ComputeMetadata> buildNodeFilter() {
        nodesFilter = new Predicate<ComputeMetadata>() {
             @Override
             public boolean apply(ComputeMetadata nodeMetadata) {
                if (nodeMetadata == null) {
                    return false;
                }
                if (tagPairs.size() > nodeMetadata.getUserMetadata().size()) {
                    return false;
                }
                for (AbstractMap.SimpleImmutableEntry entry: tagPairs) {
                    String value = nodeMetadata.getUserMetadata().get(entry.getKey());
                    if (value == null || !value.equals(entry.getValue())) {
                        return false;
                    }
                }
                return true;
            }
        };
        return nodesFilter;
    }

    public String getCredentialFromFile(String provider, String credentialPath) throws IllegalArgumentException {
        try {
            String fileContents = Files.toString(new File(credentialPath), Charsets.UTF_8);

            if (provider.equals(GOOGLE_COMPUTE_ENGINE)) {
                Supplier<Credentials> credentialSupplier = new GoogleCredentialsFromJson(fileContents);
                return credentialSupplier.get().credential;
            }

            return fileContents;
        } catch (IOException e) {
            throw new InvalidConfigurationException("Failed to retrieve the private key from the file: " + credentialPath, e);
        }
    }

    public ContextBuilder newContextBuilder(final String cloudProvider, final String identity,
                                             final String credential, final String roleName) {
        try {
            if (roleName != null && (identity != null || credential != null)) {
                throw new InvalidConfigurationException("IAM role is configured, identity "
                        + "or credential propery is not allowed.");
            }
            if (roleName != null && !cloudProvider.equals(AWS_EC2)) {
                throw new InvalidConfigurationException("IAM role is only supported with aws-ec2, your cloud "
                        + "provider is " + cloudProvider);
            }
            if (cloudProvider.equals(AWS_EC2) && roleName != null) {
                Supplier<Credentials> credentialsSupplier = new Supplier<Credentials>() {
                    @Override
                    public Credentials get() {
                        return new IAMRoleCredentialSupplierBuilder().
                                withRoleName(roleName).build();
                    }
                };
                return ContextBuilder.newBuilder(cloudProvider).credentialsSupplier(credentialsSupplier);
            } else {
                checkNotNull(identity, "Cloud provider identity is not set");
                checkNotNull(credential, "Cloud provider credential is not set");
                return ContextBuilder.newBuilder(cloudProvider).credentials(identity, credential);
            }
        } catch (NoSuchElementException e) {
            throw new InvalidConfigurationException("Unrecognized cloud-provider [" + cloudProvider + "]");
        }
    }

    private Properties newOverrideProperties() {
        Properties properties = new Properties();
        properties.setProperty(Constants.PROPERTY_SO_TIMEOUT, JCLOUD_CONNECTION_TIMEOUT);
        properties.setProperty(Constants.PROPERTY_CONNECTION_TIMEOUT, JCLOUD_CONNECTION_TIMEOUT);
        return properties;
    }

    private <T extends Comparable> T getOrNull(PropertyDefinition property) {
        return getOrDefault(property, null);
    }

    private <T extends Comparable> T getOrDefault(PropertyDefinition property, T defaultValue) {

        if (properties == null || property == null) {
            return defaultValue;
        }

        Comparable value = properties.get(property.key());
        if (value == null) {
            return defaultValue;
        }

        return (T) value;
    }

}
