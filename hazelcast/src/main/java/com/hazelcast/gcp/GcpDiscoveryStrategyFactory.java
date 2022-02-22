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

package com.hazelcast.gcp;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.utils.RestClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Factory class which returns {@link GcpDiscoveryStrategy} to Discovery SPI.
 */
public class GcpDiscoveryStrategyFactory
        implements DiscoveryStrategyFactory {
    private static final ILogger LOGGER = Logger.getLogger(GcpDiscoveryStrategyFactory.class);

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return GcpDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                  Map<String, Comparable> properties) {
        return new GcpDiscoveryStrategy(properties);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        List<PropertyDefinition> result = new ArrayList<PropertyDefinition>();
        for (GcpProperties property : GcpProperties.values()) {
            result.add(property.getDefinition());
        }
        return result;
    }

    /**
     * Checks if Hazelcast is running on GCP.
     * <p>
     * To check if Hazelcast is running on GCP, we first check whether the internal DNS is configured for "google.internal" in
     * either "/etc/resolv.conf" or "/etc/hosts". Such an approach is not officially documented but seems like a good
     * enough heuristic to detect a GCP Compute VM Instance. Since it's not the official method, we still need to make
     * an API call to "metadata.google.internal" which will resolve to a local, non-routable address http://169.254.169.254/.
     * Finally, we check if there is a service account attached for this instance because without a service account Hazelcast
     * GCP discovery will not work.
     *
     * @return true if running on GCP Instance which has a service account attached
     * @see <a href=https://cloud.google.com/compute/docs/instances/managing-instances#dmi>GCP Managing Instances</a>
     */
    @Override
    public boolean isAutoDetectionApplicable() {
        return googleInternalDnsConfigured() && metadataFlavorGoogle() && serviceAccountAttached();
    }

    private static boolean googleInternalDnsConfigured() {
        return readFileContents("/etc/resolv.conf").contains("google.internal")
                || readFileContents("/etc/hosts").contains("google.internal");
    }

    static String readFileContents(String fileName) {
        InputStream is = null;
        try {
            File file = new File(fileName);
            byte[] data = new byte[(int) file.length()];
            is = new FileInputStream(file);
            is.read(data);
            return new String(data, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Could not get " + fileName, e);
        } finally {
            IOUtil.closeResource(is);
        }
    }

    private static boolean metadataFlavorGoogle() {
        return isEndpointAvailable("http://metadata.google.internal");
    }

    private static boolean serviceAccountAttached() {
        try {
            return isEndpointAvailable("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/");
        } catch (Exception e) {
            LOGGER.warning("Hazelcast running on GCP instance, but no service account attached. Cannot use Hazelcast "
                    + "GCP discovery.");
            LOGGER.finest(e);
            return false;
        }
    }

    static boolean isEndpointAvailable(String url) {
        return !RestClient.create(url)
                .withConnectTimeoutSeconds(1)
                .withReadTimeoutSeconds(1)
                .withRetries(1)
                .withHeader("Metadata-Flavor", "Google")
                .get()
                .getBody()
                .isEmpty();
    }

    @Override
    public DiscoveryStrategyLevel discoveryStrategyLevel() {
        return DiscoveryStrategyLevel.CLOUD_VM;
    }
}
