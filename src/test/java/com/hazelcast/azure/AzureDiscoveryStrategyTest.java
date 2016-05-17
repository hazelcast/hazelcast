/*
 * Copyright (c) 2016, Microsoft Corporation. All Rights Reserved.
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

import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.test.HazelcastTestSupport;
import com.microsoft.azure.management.compute.ComputeManagementClient;
import com.microsoft.azure.management.compute.ComputeManagementService;
import com.microsoft.azure.management.compute.VirtualMachineOperations;
import com.microsoft.azure.management.compute.models.InstanceViewStatus;
import com.microsoft.azure.management.compute.models.NetworkInterfaceReference;
import com.microsoft.azure.management.compute.models.NetworkProfile;
import com.microsoft.azure.management.compute.models.VirtualMachine;
import com.microsoft.azure.management.compute.models.VirtualMachineGetResponse;
import com.microsoft.azure.management.compute.models.VirtualMachineInstanceView;
import com.microsoft.azure.management.compute.models.VirtualMachineListResponse;
import com.microsoft.azure.management.network.NetworkInterfaceOperations;
import com.microsoft.azure.management.network.NetworkResourceProviderClient;
import com.microsoft.azure.management.network.NetworkResourceProviderService;
import com.microsoft.azure.management.network.PublicIpAddressOperations;
import com.microsoft.azure.management.network.models.NetworkInterface;
import com.microsoft.azure.management.network.models.NetworkInterfaceGetResponse;
import com.microsoft.azure.management.network.models.NetworkInterfaceIpConfiguration;
import com.microsoft.azure.management.network.models.PublicIpAddress;
import com.microsoft.azure.management.network.models.PublicIpAddressDnsSettings;
import com.microsoft.azure.management.network.models.PublicIpAddressGetResponse;
import com.microsoft.azure.management.network.models.ResourceId;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.azure.AzureAuthHelper.getAzureConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(fullyQualifiedNames={
    "com.microsoft.windowsazure.core.*", 
    "com.microsoft.azure.management.compute.*", 
    "com.microsoft.azure.management.network.*",
    "com.hazelcast.azure.AzureAuthHelper"
})
public class AzureDiscoveryStrategyTest extends HazelcastTestSupport {

    private Map<String, Comparable> properties; 
    private ArrayList<VirtualMachine> virtuaMachines;
    private NetworkInterfaceOperations mockNicOps = mock(NetworkInterfaceOperations.class);
    private PublicIpAddressOperations mockPubOps = mock(PublicIpAddressOperations.class);
    private VirtualMachineOperations mockVmOps = mock(VirtualMachineOperations.class);
    {
        properties = new HashMap<String, Comparable>();
        properties.put("client-id", "test-value");
        properties.put("client-secret", "test-value");
        properties.put("subscription-id", "test-value");
        properties.put("cluster-id", "cluster000");
        properties.put("tenant-id", "test-value");
        properties.put("group-name", "test-value");
        virtuaMachines = new ArrayList<VirtualMachine>();
    }

    private final int FAULT_DOMAIN_ID = 2099;
    private final String DNS_DOMAIN_NAME = "azure1.microsoft.com";


    @Before
    public void setup() throws Exception {
        Configuration mockAzureConfig = mock(Configuration.class);
        ComputeManagementClient mockComputeManagementClient = mock(ComputeManagementClient.class);
        NetworkResourceProviderClient mockNetworkResourceProviderClient = mock(NetworkResourceProviderClient.class);
        VirtualMachineListResponse mockVmListResponse = mock(VirtualMachineListResponse.class);

        // mock all the static methods in a class called "Static"
        PowerMockito.mockStatic(AzureAuthHelper.class);
        Mockito.when(getAzureConfiguration(properties)).thenReturn(mockAzureConfig);
        PowerMockito.mockStatic(ComputeManagementService.class);
        Mockito.when(ComputeManagementService.create(mockAzureConfig)).thenReturn(mockComputeManagementClient);
        PowerMockito.mockStatic(NetworkResourceProviderService.class);
        Mockito.when(NetworkResourceProviderService.create(mockAzureConfig)).thenReturn(mockNetworkResourceProviderClient);
        Mockito.when(mockNetworkResourceProviderClient.getNetworkInterfacesOperations()).thenReturn(mockNicOps);
        Mockito.when(mockNetworkResourceProviderClient.getPublicIpAddressesOperations()).thenReturn(mockPubOps);
        Mockito.when(mockComputeManagementClient.getVirtualMachinesOperations()).thenReturn(mockVmOps);
        Mockito.when(mockVmOps.list((String)properties.get("group-name"))).thenReturn(mockVmListResponse);
        Mockito.when(mockVmListResponse.getVirtualMachines()).thenReturn(virtuaMachines);
    }

    private void buildFakeVmList(int count) throws IOException, ServiceException, URISyntaxException {
        virtuaMachines.clear();
        for (int i = 0; i < count; i++) {
            createVMWithIp(i, null);
            
        }
    }

    private void buildFakeVm(int count, String ip) throws IOException, ServiceException, URISyntaxException {
        virtuaMachines.clear();
        createVMWithIp(count, ip);
    }

    private void createVMWithIp(int i, String ipAddress) throws IOException, ServiceException, URISyntaxException {
        VirtualMachine vm = new VirtualMachine();

        // set instance status
        VirtualMachineInstanceView mockInstanceView = new VirtualMachineInstanceView();
        ArrayList<InstanceViewStatus> statuses = new ArrayList<InstanceViewStatus>();
        InstanceViewStatus mockStatus1 = new InstanceViewStatus();
        InstanceViewStatus mockStatus2 = new InstanceViewStatus();
        // this is based on what running VMs look like on resources.azure.com
        mockStatus1.setCode("PowerState/running");
        mockStatus2.setCode("ProvisioningState/succeeded");
        statuses.add(mockStatus1);
        statuses.add(mockStatus2);
        mockInstanceView.setStatuses(statuses);
        mockInstanceView.setPlatformFaultDomain(new Integer(FAULT_DOMAIN_ID));
        vm.setInstanceView(mockInstanceView);

        NetworkProfile profile = new NetworkProfile();
        NetworkInterfaceReference nir = new NetworkInterfaceReference();
        nir.setReferenceUri("/subscriptions/" + (String)properties.get("subscription-id")
            + "/resourceGroups/" + (String)properties.get("subscription-id") + "/virtualMachines/vm-" + i);
        ArrayList<NetworkInterfaceReference> nirs = new ArrayList<NetworkInterfaceReference>();
        nirs.add(nir);
        profile.setNetworkInterfaces(nirs);
        vm.setNetworkProfile(profile);

        HashMap<String, String> tags = new HashMap<String, String>();
        tags.put((String)properties.get("cluster-id"), "5701");
        vm.setTags(tags);
        virtuaMachines.add(vm);

        // we already stick the instance view on the vm, unlike the api list response
        VirtualMachineGetResponse getVmResponse = new VirtualMachineGetResponse();
        getVmResponse.setVirtualMachine(vm);
        Mockito.when(mockVmOps.getWithInstanceView((String)properties.get("group-name"), vm.getName()))
        .thenReturn(getVmResponse);
        NetworkInterface mockNic = new NetworkInterface();
        ArrayList<NetworkInterfaceIpConfiguration> ips = new ArrayList<NetworkInterfaceIpConfiguration>();
        NetworkInterfaceIpConfiguration ip = new NetworkInterfaceIpConfiguration();


        ResourceId pubIpRid = new ResourceId();
        pubIpRid.setId("/subscriptions/" + (String)properties.get("subscription-id")
            + "/resourceGroups/" + (String)properties.get("subscription-id") + "/publicIpAddresses/pubip-" + i);

        // this assumes we'll never have more than 256 fake vms
        ip.setPrivateIpAddress("10.0.0." + i);
        ip.setPublicIpAddress(pubIpRid);
        ips.add(ip);
        mockNic.setIpConfigurations(ips);


        // setup public ip address
        PublicIpAddressGetResponse mockPubIpGetResponse = new PublicIpAddressGetResponse();
        PublicIpAddress mockPubIp = new PublicIpAddress();
        PublicIpAddressDnsSettings dnsSettings = new PublicIpAddressDnsSettings();
        dnsSettings.setDomainNameLabel(DNS_DOMAIN_NAME);
        mockPubIp.setDnsSettings(dnsSettings);
        if (ipAddress == null) {
            mockPubIp.setIpAddress("44.18.12." + i);
        } else {
            mockPubIp.setIpAddress(ipAddress);
        }

        mockPubIpGetResponse.setPublicIpAddress(mockPubIp);

        Mockito.when(mockPubOps.get((String)properties.get("group-name"), "pubip-" + i)).thenReturn(mockPubIpGetResponse);

        NetworkInterfaceGetResponse mockNicGetResponse = mock(NetworkInterfaceGetResponse.class);
        Mockito.when(mockNicGetResponse.getNetworkInterface()).thenReturn(mockNic);

        Mockito.when(mockNicOps.get((String)properties.get("group-name"), "vm-" + i)).thenReturn(mockNicGetResponse);
    }

    private void testDiscoverNodesMocked(int vmCount) throws IOException, ServiceException {
        testDiscoverNodesMockedWithSkip(vmCount, -1);
    }

    private void testDiscoverNodesMockedWithSkip(int vmCount, int skipIndex) throws IOException, ServiceException {

        AzureDiscoveryStrategyFactory factory = new AzureDiscoveryStrategyFactory();
        AzureDiscoveryStrategy strategy = (AzureDiscoveryStrategy)factory.newDiscoveryStrategy(null,null, properties);

        strategy.start();
        Iterator<DiscoveryNode> nodes = strategy.discoverNodes().iterator();

        assertTrue(nodes != null);

        ArrayList<DiscoveryNode> nodeList = new ArrayList<DiscoveryNode>();
        while(nodes.hasNext()) {
            DiscoveryNode node = nodes.next();
            nodeList.add(node);
        }

        assertEquals(vmCount, nodeList.size());

        for (int i = 0; i < nodeList.size(); i++) {
            int ipSuffix = i;

            if (skipIndex != -1 && i >= skipIndex) {
                ipSuffix += 1;
            }

            assertEquals("10.0.0." + ipSuffix, nodeList.get(i).getPrivateAddress().getHost());
            assertEquals(5701, nodeList.get(i).getPrivateAddress().getPort());

            assertEquals("44.18.12." + ipSuffix, nodeList.get(i).getPublicAddress().getHost());
            assertEquals(5701, nodeList.get(i).getPublicAddress().getPort());
        }
    }

    @Test
    public void testDiscoverNodesMocked255() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(255);
        testDiscoverNodesMocked(255);
    }

    @Test
    public void testDiscoverNodesMetadata() throws IOException, ServiceException, URISyntaxException {
        AzureDiscoveryStrategyFactory factory = new AzureDiscoveryStrategyFactory();
        AzureDiscoveryStrategy strategy = (AzureDiscoveryStrategy)factory.newDiscoveryStrategy(null,null, properties);
        strategy.start();
        String localIp = strategy.getLocalHostAddress();
        buildFakeVm(0, localIp);
        strategy.discoverNodes();

        assertEquals(strategy.discoverLocalMetadata().get(PartitionGroupMetaData.PARTITION_GROUP_ZONE),
                new Integer(FAULT_DOMAIN_ID).toString());

        assertEquals(strategy.discoverLocalMetadata().get(PartitionGroupMetaData.PARTITION_GROUP_HOST),
                DNS_DOMAIN_NAME);
    }

    @Test
    public void testDiscoverNodesMocked3() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(3);
        testDiscoverNodesMocked(3);
    }

    @Test
    public void testDiscoverNodesMocked1() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(1);
        testDiscoverNodesMocked(1);
    }

    @Test
    public void testDiscoverNodesMocked_0() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(0);
        testDiscoverNodesMocked(0);
    }

    @Test
    public void testFaultDomainIsSet() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(0);
        testDiscoverNodesMocked(0);
    }

    @Test
    public void testDiscoverNodesStoppedVM() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(4);
        VirtualMachine vmToTurnOff = virtuaMachines.remove(2);
        // turn off the vm
        VirtualMachineInstanceView newView = new VirtualMachineInstanceView();
        ArrayList<InstanceViewStatus> statuses = new ArrayList<InstanceViewStatus>();
        InstanceViewStatus status1 = new InstanceViewStatus();
        InstanceViewStatus status2 = new InstanceViewStatus();
        statuses.add(status1);
        statuses.add(status2);
        newView.setStatuses(statuses);
        status1.setCode("PowerState/deallocated");
        status2.setCode("ProvisioningState/succeeded");
        vmToTurnOff.setInstanceView(newView);
        // should only recognize 3 hazelcast instances now
        testDiscoverNodesMockedWithSkip(3, 2);
    }

    @Test
    public void testDiscoverNodesUntaggedVM() throws IOException, ServiceException, URISyntaxException {
        buildFakeVmList(6);
        VirtualMachine vmToUntag = virtuaMachines.get(3);
        
        // retag vm
        HashMap<String, String> newTags = new HashMap<String, String>();
        newTags.put("INVALID_TAG", "INVALID_PORT");
        vmToUntag.setTags(newTags);

        // should only recognize 5 hazelcast instances now
        testDiscoverNodesMockedWithSkip(5, 3);
    }
}
