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

package com.hazelcast.aws;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@RunWith(HazelcastParallelClassRunner.class)
@Category( {QuickTest.class, ParallelJVMTest.class})
public class AwsDescribeInstancesApiTest {
    public static final String DUMMY_PRIVATE_IP = "10.0.0.1";
    private static final String REGION = "us-east-1";
    private static final String HOST_HEADER = "ec2.amazonaws.com";

    private static InputStream toInputStream(String s)
        throws Exception {
        return new ByteArrayInputStream(s.getBytes("UTF-8"));
    }

    @Test
    public void test_CheckNoAwsErrors_NoAwsErrors()
        throws Exception {
        // given
        int httpResponseCode = 200;

        HttpURLConnection httpConnection = mock(HttpURLConnection.class);
        given(httpConnection.getResponseCode()).willReturn(httpResponseCode);

        AwsConfig awsConfig = predefinedAwsConfigBuilder().build();
        AwsDescribeInstancesApi awsDescribeInstancesApi = new AwsDescribeInstancesApi(awsConfig);

        // when
        awsDescribeInstancesApi.checkNoAwsErrors(httpConnection);

        // then
        // no exceptions thrown
    }

    @Test
    public void test_CheckNoAwsErrors_ConnectionFailed()
        throws Exception {
        // given
        int httpResponseCode = 401;
        String errorMessage = "Error message retrived from AWS";

        HttpURLConnection httpConnection = mock(HttpURLConnection.class);
        given(httpConnection.getResponseCode()).willReturn(httpResponseCode);
        given(httpConnection.getErrorStream()).willReturn(toInputStream(errorMessage));

        AwsConfig awsConfig = predefinedAwsConfigBuilder().build();
        AwsDescribeInstancesApi awsDescribeInstancesApi = new AwsDescribeInstancesApi(awsConfig);

        // when & then
        try {
            awsDescribeInstancesApi.checkNoAwsErrors(httpConnection);
            fail("AwsConnectionFailed exception was not thrown");
        } catch (AwsConnectionException e) {
            assertEquals(httpResponseCode, e.getHttpReponseCode());
            assertEquals(errorMessage, e.getErrorMessage());
            assertThat(e.getMessage(), containsString(Integer.toString(httpResponseCode)));
            assertThat(e.getMessage(), containsString(errorMessage));

        }
    }

    @Test
    public void test_CheckNoAwsErrors_ConnectionFailedAndNullErrorStream()
        throws Exception {
        // given
        int httpResponseCode = 401;

        HttpURLConnection httpConnection = mock(HttpURLConnection.class);
        given(httpConnection.getResponseCode()).willReturn(httpResponseCode);
        given(httpConnection.getErrorStream()).willReturn(null);

        AwsConfig awsConfig = predefinedAwsConfigBuilder().build();
        AwsDescribeInstancesApi awsDescribeInstancesApi = new AwsDescribeInstancesApi(awsConfig);

        // when & then
        try {
            awsDescribeInstancesApi.checkNoAwsErrors(httpConnection);
            fail("AwsConnectionFailed exception was not thrown");
        } catch (AwsConnectionException e) {
            assertEquals(httpResponseCode, e.getHttpReponseCode());
            assertEquals("", e.getErrorMessage());
            assertThat(e.getMessage(), containsString(Integer.toString(httpResponseCode)));
        }
    }

    @Test
    public void test_Addresses()
        throws Exception {
        // given
        AwsConfig awsConfig = predefinedAwsConfigBuilder().build();
        AwsCredentials credentials = AwsCredentials.builder()
            .setAccessKey("dummyAccessKey")
            .setSecretKey("dummySecretKey")
            .build();

        AwsDescribeInstancesApi describeInstances = spy(new AwsDescribeInstancesApi(awsConfig));
        doReturn(stubDescribeInstancesResponse()).when(describeInstances).callService(any(), any(), any());

        // when
        Map<String, String> result = describeInstances.addresses(REGION, HOST_HEADER, credentials);

        // then
        Assert.assertNotNull(result);
        assertEquals(1, result.size());
        Assert.assertNotNull(result.get(DUMMY_PRIVATE_IP));
    }

    private InputStream stubDescribeInstancesResponse() {
        String response = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">\n"
            + "    <requestId>02cc8d1a-a4c4-46d4-b8da-b82c38a443a1</requestId>\n" + "    <reservationSet>\n"
            + "        <item>\n" + "            <reservationId>r-06667d2ad4b67d67d</reservationId>\n"
            + "            <ownerId>665466731577</ownerId>\n" + "            <groupSet/>\n" + "            <instancesSet>\n"
            + "                <item>\n" + "                    <instanceId>i-0d5215783d8c7ce4b</instanceId>\n"
            + "                    <imageId>ami-bc4052c6</imageId>\n" + "                    <instanceState>\n"
            + "                        <code>16</code>\n" + "                        <name>running</name>\n"
            + "                    </instanceState>\n"
            + "                    <privateDnsName>ip-172-30-0-135.ec2.internal</privateDnsName>\n"
            + "                    <dnsName>ec2-34-229-19-19.compute-1.amazonaws.com</dnsName>\n"
            + "                    <reason/>\n" + "                    <keyName>cpp-release</keyName>\n"
            + "                    <amiLaunchIndex>0</amiLaunchIndex>\n" + "                    <productCodes/>\n"
            + "                    <instanceType>t2.xlarge</instanceType>\n"
            + "                    <launchTime>2018-05-14T11:15:47.000Z</launchTime>\n" + "                    <placement>\n"
            + "                        <availabilityZone>us-east-1a</availabilityZone>\n"
            + "                        <groupName/>\n" + "                        <tenancy>default</tenancy>\n"
            + "                    </placement>\n" + "                    <platform>windows</platform>\n"
            + "                    <monitoring>\n" + "                        <state>disabled</state>\n"
            + "                    </monitoring>\n" + "                    <subnetId>subnet-89679cfe</subnetId>\n"
            + "                    <vpcId>vpc-26c61c43</vpcId>\n"
            + "                    <privateIpAddress>%s</privateIpAddress>\n"
            + "                    <ipAddress>34.229.19.19</ipAddress>\n"
            + "                    <sourceDestCheck>true</sourceDestCheck>\n" + "                    <groupSet>\n"
            + "                        <item>\n" + "                            <groupId>sg-fddc2b82</groupId>\n"
            + "                            <groupName>launch-wizard-147</groupName>\n" + "                        </item>\n"
            + "                    </groupSet>\n" + "                    <architecture>x86_64</architecture>\n"
            + "                    <rootDeviceType>ebs</rootDeviceType>\n"
            + "                    <rootDeviceName>/dev/sda1</rootDeviceName>\n"
            + "                    <blockDeviceMapping>\n" + "                        <item>\n"
            + "                            <deviceName>/dev/sda1</deviceName>\n" + "                            <ebs>\n"
            + "                                <volumeId>vol-0371f04cfabfee833</volumeId>\n"
            + "                                <status>attached</status>\n"
            + "                                <attachTime>2018-05-12T08:34:46.000Z</attachTime>\n"
            + "                                <deleteOnTermination>false</deleteOnTermination>\n"
            + "                            </ebs>\n" + "                        </item>\n"
            + "                    </blockDeviceMapping>\n"
            + "                    <virtualizationType>hvm</virtualizationType>\n" + "                    <clientToken/>\n"
            + "                    <tagSet>\n" + "                        <item>\n"
            + "                            <key>Name</key>\n"
            + "                            <value>*windows-jenkins-cpp</value>\n" + "                        </item>\n"
            + "                        <item>\n" + "                            <key>aws-test-tag</key>\n"
            + "                            <value>aws-tag-value-1</value>\n" + "                        </item>\n"
            + "                    </tagSet>\n" + "                    <hypervisor>xen</hypervisor>\n"
            + "                    <networkInterfaceSet>\n" + "                        <item>\n"
            + "                            <networkInterfaceId>eni-5f8420c1</networkInterfaceId>\n"
            + "                            <subnetId>subnet-89679cfe</subnetId>\n"
            + "                            <vpcId>vpc-26c61c43</vpcId>\n"
            + "                            <description>Primary network interface</description>\n"
            + "                            <ownerId>665466731577</ownerId>\n"
            + "                            <status>in-use</status>\n"
            + "                            <macAddress>0a:50:3a:ec:bf:26</macAddress>\n"
            + "                            <privateIpAddress>%s</privateIpAddress>\n"
            + "                            <privateDnsName>ip-172-30-0-135.ec2.internal</privateDnsName>\n"
            + "                            <sourceDestCheck>true</sourceDestCheck>\n"
            + "                            <groupSet>\n" + "                                <item>\n"
            + "                                    <groupId>sg-fddc2b82</groupId>\n"
            + "                                    <groupName>launch-wizard-147</groupName>\n"
            + "                                </item>\n" + "                            </groupSet>\n"
            + "                            <attachment>\n"
            + "                                <attachmentId>eni-attach-92200632</attachmentId>\n"
            + "                                <deviceIndex>0</deviceIndex>\n"
            + "                                <status>attached</status>\n"
            + "                                <attachTime>2018-05-12T08:34:46.000Z</attachTime>\n"
            + "                                <deleteOnTermination>true</deleteOnTermination>\n"
            + "                            </attachment>\n" + "                            <association>\n"
            + "                                <publicIp>34.229.19.19</publicIp>\n"
            + "                                <publicDnsName>ec2-34-229-19-19.compute-1.amazonaws.com</publicDnsName>\n"
            + "                                <ipOwnerId>amazon</ipOwnerId>\n"
            + "                            </association>\n" + "                            <privateIpAddressesSet>\n"
            + "                                <item>\n"
            + "                                    <privateIpAddress>%s</privateIpAddress>\n"
            + "                                    <privateDnsName>ip-172-30-0-135.ec2.internal</privateDnsName>\n"
            + "                                    <primary>true</primary>\n"
            + "                                    <association>\n"
            + "                                    <publicIp>34.229.19.19</publicIp>\n"
            + "                                    <publicDnsName>ec2-34-229-19-19.compute-1.amazonaws.com</publicDnsName>\n"
            + "                                    <ipOwnerId>amazon</ipOwnerId>\n"
            + "                                    </association>\n" + "                                </item>\n"
            + "                            </privateIpAddressesSet>\n" + "                            <ipv6AddressesSet/>\n"
            + "                        </item>\n" + "                    </networkInterfaceSet>\n"
            + "                    <iamInstanceProfile>\n"
            + "                        <arn>arn:aws:iam::665466731577:instance-profile/cloudbees-role</arn>\n"
            + "                        <id>AIPAIFCB7AIF75MJZDAOO</id>\n" + "                    </iamInstanceProfile>\n"
            + "                    <ebsOptimized>false</ebsOptimized>\n" + "                    <cpuOptions>\n"
            + "                        <coreCount>4</coreCount>\n"
            + "                        <threadsPerCore>1</threadsPerCore>\n" + "                    </cpuOptions>\n"
            + "                </item>\n" + "            </instancesSet>\n" + "        </item>\n" + "    </reservationSet>\n"
            + "</DescribeInstancesResponse>", DUMMY_PRIVATE_IP, DUMMY_PRIVATE_IP, DUMMY_PRIVATE_IP);
        return new ByteArrayInputStream(response.getBytes());
    }

    private static AwsConfig.Builder predefinedAwsConfigBuilder() {
        return AwsConfig.builder()
            .setHostHeader(HOST_HEADER)
            .setRegion(REGION)
            .setConnectionTimeoutSeconds(5);
    }
}
