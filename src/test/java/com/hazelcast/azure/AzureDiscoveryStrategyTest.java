package com.hazelcast.azure.test;

import com.hazelcast.azure.AzureDiscoveryStrategy;
import com.hazelcast.azure.AzureDiscoveryStrategyFactory;
import com.hazelcast.azure.AzureAuthHelper;

import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;

import com.hazelcast.spi.discovery.DiscoveryNode;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.ValidationException;

import com.microsoft.windowsazure.core.*;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.Configuration;

import com.microsoft.azure.management.compute.*;
import com.microsoft.azure.management.compute.models.*;

import com.microsoft.azure.management.network.*;
import com.microsoft.azure.management.network.models.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

import java.net.URISyntaxException;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static com.hazelcast.util.StringUtil.stringToBytes;

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

    @Before
    public void setup() throws Exception {
        Configuration mockAzureConfig = mock(Configuration.class);
        ComputeManagementClient mockComputeManagementClient = mock(ComputeManagementClient.class);
        NetworkResourceProviderClient mockNetworkResourceProviderClient = mock(NetworkResourceProviderClient.class);
        VirtualMachineListResponse mockVmListResponse = mock(VirtualMachineListResponse.class);

        // mock all the static methods in a class called "Static"
        PowerMockito.mockStatic(AzureAuthHelper.class);
        Mockito.when(AzureAuthHelper.getAzureConfiguration(properties)).thenReturn(mockAzureConfig);
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
            mockPubIp.setIpAddress("44.18.12." + i);
            mockPubIpGetResponse.setPublicIpAddress(mockPubIp);
            
            Mockito.when(mockPubOps.get((String)properties.get("group-name"), "pubip-" + i)).thenReturn(mockPubIpGetResponse);

            NetworkInterfaceGetResponse mockNicGetResponse = mock(NetworkInterfaceGetResponse.class);
            Mockito.when(mockNicGetResponse.getNetworkInterface()).thenReturn(mockNic);

            Mockito.when(mockNicOps.get((String)properties.get("group-name"), "vm-" + i)).thenReturn(mockNicGetResponse);
            
        }
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
