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

package com.hazelcast.azure;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import com.microsoft.azure.management.resources.ResourceManagementService;
import com.microsoft.azure.management.resources.ResourceManagementClient;

import com.microsoft.azure.management.compute.VirtualMachineOperations;
import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.models.NetworkProfile;
import com.microsoft.azure.management.compute.models.NetworkInterfaceReference;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.compute.models.VirtualMachineListResponse;
import com.microsoft.azure.management.compute.ComputeManagementService;

import com.microsoft.azure.management.network.NetworkInterfaceOperations;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.microsoft.azure.management.network.models.NetworkInterface;
import com.microsoft.azure.management.network.models.PublicIpAddress;
import com.microsoft.azure.management.network.PublicIpAddressOperations;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.io.IOException;

/**
 * Azure implementation of {@link DiscoveryStrategy}
 */
public class AzureDiscoveryStrategy implements DiscoveryStrategy {

    private static final ILogger LOGGER = Logger.getLogger(AzureDiscoveryStrategy.class);
    
    private ComputeManagementClient computeManagement;
    private NetworkResourceProviderClient networkManagement;
    private Map<String, Comparable> properties;

    /**
     * Instantiates a new AzureDiscoveryStrategy
     *
     * @param properties the properties
     */
    public AzureDiscoveryStrategy(Map<String, Comparable> properties) {
         this.properties = properties;
    }

    @Override
    public void start() {
        try {
            Configuration config = AzureAuthHelper.getAzureConfiguration(this.properties);
            this.computeManagement = ComputeManagementService.create(config);
            this.networkManagement = NetworkResourceProviderService.create(config);
        }
        catch (Exception e) {
            LOGGER.finest("Failed to start Azure SPI", e);
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        try {
            VirtualMachineOperations vmOps = this.computeManagement.getVirtualMachinesOperations();
       
            String resourceGroup = AzureProperties.getOrNull(AzureProperties.GROUP_NAME, properties);
            String clusterId = AzureProperties.getOrNull(AzureProperties.HZLCST_CLUSTER_ID, properties);

            VirtualMachineListResponse vms = vmOps.list(resourceGroup);

            ArrayList<DiscoveryNode> nodes = new ArrayList<DiscoveryNode>();

            for (VirtualMachine vm : vms.getVirtualMachines()) {
                NetworkProfile netProfile = vm.getNetworkProfile();
                HashMap<String, String> tags = vm.getTags();
                
                // a tag is required with the hazelcast clusterid
                // and the value should be the port number
                if (tags.get(clusterId) == null) {
                    continue;
                }

                int port = Integer.parseInt(tags.get(clusterId));
                DiscoveryNode node = buildDiscoveredNode(netProfile, port);
                
                if (node != null) {
                    nodes.add(node);
                }
            }
            return nodes;
        }
        catch (Exception e) {
            e.printStackTrace();
            LOGGER.finest("Failed to discover nodes with Azure SPI", e);
            return null;
        }
        
    }

    @Override
    public void destroy() {
        // no native resources were allocated so nothing to do here
    }

    /**
    * Takes a reference URI like:
    * /subscriptions/{SubcriptionId}/resourceGroups/{ResourceGroupName}/...
    * and returns the resource name
    * 
    * @param referenceUri reference uri of resource
    */
    private String getResourceNameFromUri(String referenceUri) {
        String[] parts = referenceUri.split("/");
        String name = parts[parts.length - 1];
        return name;
    }

    private DiscoveryNode buildDiscoveredNode(NetworkProfile profile, int port) throws UnknownHostException, IOException, ServiceException, Exception {
        PublicIpAddressOperations pubOps = this.networkManagement.getPublicIpAddressesOperations();
        String rgName = AzureProperties.getOrNull(AzureProperties.GROUP_NAME, properties);
        NetworkInterfaceOperations nicOps = this.networkManagement.getNetworkInterfacesOperations();

        for (NetworkInterfaceReference nir : profile.getNetworkInterfaces()) {
            // This SPI interface doesn't accomidate for
            // multiple  NICs, so only use primary NICs
            if (nir.isPrimary() != null && !nir.isPrimary()) {
                continue;
            }
            String uri = nir.getReferenceUri();
            String nicName = getResourceNameFromUri(uri);
            NetworkInterface nic = nicOps.get(rgName, nicName).getNetworkInterface();
            ArrayList<NetworkInterfaceIpConfiguration> ips =  nic.getIpConfigurations();

            // TODO is it possilbe for NIC to have > 1
            // IP address configuration?
            if (ips.size() == 0) {
                continue;
            }

            NetworkInterfaceIpConfiguration ip = ips.get(0);
            Address privateAddress = new Address(ip.getPrivateIpAddress(), port);
            Address publicAddress;
            // Public IPs are resources, so we need to query
            // the value of the IP if it exists
            if (ip.getPublicIpAddress() != null) {
                String id = ip.getPublicIpAddress().getId();
                String pubIpName = getResourceNameFromUri(id);
                PublicIpAddress pubIp = pubOps.get(rgName, pubIpName).getPublicIpAddress();
                publicAddress = new Address(pubIp.getIpAddress(), port);
                return new SimpleDiscoveryNode(privateAddress, publicAddress);
            }
            return new SimpleDiscoveryNode(privateAddress);
            
        }

        // no node found;
        return null;
    }
}