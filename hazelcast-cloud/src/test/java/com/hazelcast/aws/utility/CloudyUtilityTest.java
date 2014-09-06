/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws.utility;

import com.hazelcast.config.AwsConfig;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CloudyUtilityTest {
    String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
            "<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2011-05-15/\">\n" +
            "    <requestId>c0f82bf8-b7f5-4cf1-bbfa-b95ea4bd38da</requestId>\n" +
            "    <reservationSet>\n" +
            "        <item>\n" +
            "            <reservationId>r-48ff3826</reservationId>\n" +
            "            <ownerId>665466731577</ownerId>\n" +
            "            <groupSet>\n" +
            "                <item>\n" +
            "                    <groupId>sg-b67baddf</groupId>\n" +
            "                    <groupName>hazelcast</groupName>\n" +
            "                </item>\n" +
            "            </groupSet>\n" +
            "            <instancesSet>\n" +
            "                <item>\n" +
            "                    <instanceId>i-0a0c616a</instanceId>\n" +
            "                    <imageId>ami-7f418316</imageId>\n" +
            "                    <instanceState>\n" +
            "                        <code>16</code>\n" +
            "                        <name>running</name>\n" +
            "                    </instanceState>\n" +
            "                    <privateDnsName>domU-12-31-39-07-C5-C4.compute-1.internal</privateDnsName>\n" +
            "                    <dnsName>ec2-50-17-19-37.compute-1.amazonaws.com</dnsName>\n" +
            "                    <reason/>\n" +
            "                    <keyName>hazelcast_key_pair</keyName>\n" +
            "                    <amiLaunchIndex>0</amiLaunchIndex>\n" +
            "                    <productCodes/>\n" +
            "                    <instanceType>t1.micro</instanceType>\n" +
            "                    <launchTime>2011-09-27T11:37:35.000Z</launchTime>\n" +
            "                    <placement>\n" +
            "                        <availabilityZone>us-east-1a</availabilityZone>\n" +
            "                        <groupName/>\n" +
            "                        <tenancy>default</tenancy>\n" +
            "                    </placement>\n" +
            "                    <kernelId>aki-805ea7e9</kernelId>\n" +
            "                    <monitoring>\n" +
            "                        <state>disabled</state>\n" +
            "                    </monitoring>\n" +
            "                    <privateIpAddress>10.209.198.50</privateIpAddress>\n" +
            "                    <ipAddress>50.17.19.37</ipAddress>\n" +
            "                    <groupSet>\n" +
            "                        <item>\n" +
            "                            <groupId>sg-b67baddf</groupId>\n" +
            "                            <groupName>hazelcast</groupName>\n" +
            "                        </item>\n" +
            "                    </groupSet>\n" +
            "                    <architecture>i386</architecture>\n" +
            "                    <rootDeviceType>ebs</rootDeviceType>\n" +
            "                    <rootDeviceName>/dev/sda1</rootDeviceName>\n" +
            "                    <blockDeviceMapping>\n" +
            "                        <item>\n" +
            "                            <deviceName>/dev/sda1</deviceName>\n" +
            "                            <ebs>\n" +
            "                                <volumeId>vol-d5bdffbf</volumeId>\n" +
            "                                <status>attached</status>\n" +
            "                                <attachTime>2011-09-27T11:37:56.000Z</attachTime>\n" +
            "                                <deleteOnTermination>true</deleteOnTermination>\n" +
            "                            </ebs>\n" +
            "                        </item>\n" +
            "                    </blockDeviceMapping>\n" +
            "                    <virtualizationType>paravirtual</virtualizationType>\n" +
            "                    <clientToken/>\n" +
            "                    <tagSet>\n" +
            "                        <item>\n" +
            "                            <key>name2</key>\n" +
            "                            <value>value2</value>\n" +
            "                        </item>\n" +
            "                        <item>\n" +
            "                            <key>Name1</key>\n" +
            "                            <value>value1</value>\n" +
            "                        </item>\n" +
            "                        <item>\n" +
            "                            <key>name</key>\n" +
            "                            <value/>\n" +
            "                        </item>\n" +
            "                    </tagSet>\n" +
            "                    <hypervisor>xen</hypervisor>\n" +
            "                </item>\n" +
            "                <item>\n" +
            "                    <instanceId>i-0c0c616c</instanceId>\n" +
            "                    <imageId>ami-7f418316</imageId>\n" +
            "                    <instanceState>\n" +
            "                        <code>16</code>\n" +
            "                        <name>running</name>\n" +
            "                    </instanceState>\n" +
            "                    <privateDnsName>domU-12-31-39-07-C2-60.compute-1.internal</privateDnsName>\n" +
            "                    <dnsName>ec2-50-16-102-143.compute-1.amazonaws.com</dnsName>\n" +
            "                    <reason/>\n" +
            "                    <keyName>hazelcast_key_pair</keyName>\n" +
            "                    <amiLaunchIndex>1</amiLaunchIndex>\n" +
            "                    <productCodes/>\n" +
            "                    <instanceType>t1.micro</instanceType>\n" +
            "                    <launchTime>2011-09-27T11:37:35.000Z</launchTime>\n" +
            "                    <placement>\n" +
            "                        <availabilityZone>us-east-1a</availabilityZone>\n" +
            "                        <groupName/>\n" +
            "                        <tenancy>default</tenancy>\n" +
            "                    </placement>\n" +
            "                    <kernelId>aki-805ea7e9</kernelId>\n" +
            "                    <monitoring>\n" +
            "                        <state>disabled</state>\n" +
            "                    </monitoring>\n" +
            "                    <privateIpAddress>10.209.193.170</privateIpAddress>\n" +
            "                    <ipAddress>50.16.102.143</ipAddress>\n" +
            "                    <groupSet>\n" +
            "                        <item>\n" +
            "                            <groupId>sg-b67baddf</groupId>\n" +
            "                            <groupName>hazelcast</groupName>\n" +
            "                        </item>\n" +
            "                    </groupSet>\n" +
            "                    <architecture>i386</architecture>\n" +
            "                    <rootDeviceType>ebs</rootDeviceType>\n" +
            "                    <rootDeviceName>/dev/sda1</rootDeviceName>\n" +
            "                    <blockDeviceMapping>\n" +
            "                        <item>\n" +
            "                            <deviceName>/dev/sda1</deviceName>\n" +
            "                            <ebs>\n" +
            "                                <volumeId>vol-abbdffc1</volumeId>\n" +
            "                                <status>attached</status>\n" +
            "                                <attachTime>2011-09-27T11:37:57.000Z</attachTime>\n" +
            "                                <deleteOnTermination>true</deleteOnTermination>\n" +
            "                            </ebs>\n" +
            "                        </item>\n" +
            "                    </blockDeviceMapping>\n" +
            "                    <virtualizationType>paravirtual</virtualizationType>\n" +
            "                    <clientToken/>\n" +
            "                    <tagSet>\n" +
            "                        <item>\n" +
            "                            <key>Name1</key>\n" +
            "                            <value>value1</value>\n" +
            "                        </item>\n" +
            "                        <item>\n" +
            "                            <key>name2</key>\n" +
            "                            <value>value2</value>\n" +
            "                        </item>\n" +
            "                    </tagSet>\n" +
            "                    <hypervisor>xen</hypervisor>\n" +
            "                </item>\n" +
            "            </instancesSet>\n" +
            "            <requesterId>058890971305</requesterId>\n" +
            "        </item>\n" +
            "    </reservationSet>\n" +
            "</DescribeInstancesResponse>";

    @Test
    public void testNoTags() throws IOException {
        InputStream is = new ByteArrayInputStream(xml.getBytes());
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setAccessKey("some-access-key");
        awsConfig.setSecretKey("some-secret-key");
        awsConfig.setSecurityGroupName("hazelcast");
        final Map<String, String> result = CloudyUtility.unmarshalTheResponse(is, awsConfig);
        assertEquals(2, result.size());
    }

    @Test
    public void testTagsBothNodeHave() throws IOException {
        InputStream is = new ByteArrayInputStream(xml.getBytes());
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setAccessKey("some-access-key");
        awsConfig.setSecretKey("some-secret-key");
        awsConfig.setSecurityGroupName("hazelcast");
        awsConfig.setTagKey("Name1");
        awsConfig.setTagValue("value1");
        final Map<String, String> result = CloudyUtility.unmarshalTheResponse(is, awsConfig);
        assertEquals(2, result.size());
    }

    @Test
    public void testTagOnlyOneNodeHave() throws IOException {
        InputStream is = new ByteArrayInputStream(xml.getBytes());
        AwsConfig awsConfig = new AwsConfig();
        awsConfig.setAccessKey("some-access-key");
        awsConfig.setSecretKey("some-secret-key");
        awsConfig.setSecurityGroupName("hazelcast");
        awsConfig.setTagKey("name");
        awsConfig.setTagValue("");
        final Map<String, String> result = CloudyUtility.unmarshalTheResponse(is, awsConfig);
        assertEquals(1, result.size());
    }
}
