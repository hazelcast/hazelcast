/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws.impl;

import com.hazelcast.aws.AwsConfig;
import com.hazelcast.aws.exception.AwsConnectionException;
import com.hazelcast.aws.utility.Environment;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Map;

import static com.hazelcast.aws.utility.MetadataUtil.IAM_SECURITY_CREDENTIALS_URI;
import static com.hazelcast.aws.utility.MetadataUtil.INSTANCE_METADATA_URI;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DescribeInstancesTest {
    private static final String DUMMY_ACCESS_KEY = "DUMMY_ACCESS_KEY";
    private static final String DUMMY_SECRET_KEY = "DUMMY_SECRET_ACCESS_KEY";
    private static final String DUMMY_TOKEN = "DUMMY_TOKEN";
    private static final String DUMMY_IAM_ROLE =
            "        {\n" + "          \"Code\" : \"Success\",\n" + "          \"LastUpdated\" : \"2016-10-04T12:08:24Z\",\n"
                    + "          \"Type\" : \"AWS-HMAC\",\n" + "          \"AccessKeyId\" : \"" + DUMMY_ACCESS_KEY + "\",\n"
                    + "          \"SecretAccessKey\" : \"" + DUMMY_SECRET_KEY + "\",\n" + "          \"Token\" : \"" + DUMMY_TOKEN
                    + "\",\n" + "          \"Expiration\" : \"2016-10-04T18:19:39Z\"\n" + "        }\n";
    private static final String HOST_HEADER = "ec2.amazonaws.com";
    public static final String DUMMY_PRIVATE_IP = "10.0.0.1";

    @Test(expected = IllegalArgumentException.class)
    public void test_whenAccessKey_And_IamRole_And_IamTaskRoleEnvVar_Null_With_No_DefaultRole()
            throws IOException {
        Environment mockedEnv = mock(Environment.class);
        when(mockedEnv.getEnvVar(Constants.ECS_CREDENTIALS_ENV_VAR_NAME)).thenReturn(null);

        final String uri = INSTANCE_METADATA_URI + IAM_SECURITY_CREDENTIALS_URI;

        DescribeInstances descriptor = spy(new DescribeInstances(predefinedAwsConfigBuilder().build()));
        doReturn("").when(descriptor).retrieveRoleFromURI(uri);
        doReturn(mockedEnv).when(descriptor).getEnvironment();
        descriptor.fillKeysFromIamRoles();
    }

    @Test
    public void test_whenAccessKey_And_IamTaskRoleEnvVar_Null_With_DefaultRole_Assigned()
            throws IOException {
        Environment mockedEnv = mock(Environment.class);
        when(mockedEnv.getEnvVar(Constants.ECS_CREDENTIALS_ENV_VAR_NAME)).thenReturn(null);

        final String defaultIamRoleName = "defaultIamRole";
        final String uri = INSTANCE_METADATA_URI + IAM_SECURITY_CREDENTIALS_URI;

        final String roleUri = INSTANCE_METADATA_URI + IAM_SECURITY_CREDENTIALS_URI + defaultIamRoleName;

        // test when <iam-role>DEFAULT</iam-role>
        AwsConfig awsConfig = predefinedAwsConfigBuilder().setIamRole("DEFAULT").build();

        DescribeInstances descriptor = spy(new DescribeInstances(awsConfig));
        doReturn(defaultIamRoleName).when(descriptor).retrieveRoleFromURI(uri);
        doReturn(DUMMY_IAM_ROLE).when(descriptor).retrieveRoleFromURI(roleUri);
        doReturn(mockedEnv).when(descriptor).getEnvironment();
        descriptor.fillKeysFromIamRoles();

        assertEquals("Could not parse access key from IAM role", DUMMY_ACCESS_KEY, awsConfig.getAccessKey());
        assertEquals("Could not parse secret key from IAM role", DUMMY_SECRET_KEY, awsConfig.getSecretKey());

        // test when <iam-role></iam-role>
        awsConfig = predefinedAwsConfigBuilder().setIamRole("").build();

        descriptor = spy(new DescribeInstances(awsConfig));
        doReturn(defaultIamRoleName).when(descriptor).retrieveRoleFromURI(uri);
        doReturn(DUMMY_IAM_ROLE).when(descriptor).retrieveRoleFromURI(roleUri);
        descriptor.fillKeysFromIamRoles();

        assertEquals("Could not parse access key from IAM role", DUMMY_ACCESS_KEY, awsConfig.getAccessKey());
        assertEquals("Could not parse secret key from IAM role", DUMMY_SECRET_KEY, awsConfig.getSecretKey());

        // test when no <iam-role></iam-role> defined, BUT default IAM role has been assigned
        awsConfig = predefinedAwsConfigBuilder().build();

        descriptor = spy(new DescribeInstances(awsConfig));
        doReturn(defaultIamRoleName).when(descriptor).retrieveRoleFromURI(uri);
        doReturn(DUMMY_IAM_ROLE).when(descriptor).retrieveRoleFromURI(roleUri);
        descriptor.fillKeysFromIamRoles();

        assertEquals("Could not parse access key from IAM role", DUMMY_ACCESS_KEY, awsConfig.getAccessKey());
        assertEquals("Could not parse secret key from IAM role", DUMMY_SECRET_KEY, awsConfig.getSecretKey());

    }

    @Test
    public void test_whenIamRoleExistsInConfig()
            throws IOException {
        final String someRole = "someRole";
        final String uri = INSTANCE_METADATA_URI + IAM_SECURITY_CREDENTIALS_URI + someRole;

        AwsConfig awsConfig = predefinedAwsConfigBuilder().setIamRole(someRole).build();

        DescribeInstances descriptor = spy(new DescribeInstances(awsConfig));
        doReturn(DUMMY_IAM_ROLE).when(descriptor).retrieveRoleFromURI(uri);
        descriptor.fillKeysFromIamRoles();

        assertEquals("Could not parse access key from IAM role", DUMMY_ACCESS_KEY, awsConfig.getAccessKey());
        assertEquals("Could not parse secret key from IAM role", DUMMY_SECRET_KEY, awsConfig.getSecretKey());

    }

    @Test
    public void test_when_Empty_IamRole_And_DefaultIamRole_But_IamTaskRoleEnvVar_Exists()
            throws IOException {
        final String ecsEnvVarCredsUri = "someURL";
        final String uri = DescribeInstances.IAM_TASK_ROLE_ENDPOINT + ecsEnvVarCredsUri;
        final String defaultRoleUri = INSTANCE_METADATA_URI + IAM_SECURITY_CREDENTIALS_URI;

        Environment mockedEnv = mock(Environment.class);
        when(mockedEnv.getEnvVar(Constants.ECS_CREDENTIALS_ENV_VAR_NAME)).thenReturn(ecsEnvVarCredsUri);

        AwsConfig awsConfig = predefinedAwsConfigBuilder().build();

        // test when default role is null
        DescribeInstances descriptor = spy(new DescribeInstances(awsConfig));
        doReturn(DUMMY_IAM_ROLE).when(descriptor).retrieveRoleFromURI(uri);
        doReturn("").when(descriptor).retrieveRoleFromURI(defaultRoleUri);
        doReturn(mockedEnv).when(descriptor).getEnvironment();

        descriptor.fillKeysFromIamRoles();

        assertEquals("Could not parse access key from IAM task role", DUMMY_ACCESS_KEY, awsConfig.getAccessKey());
        assertEquals("Could not parse secret key from IAM task role", DUMMY_SECRET_KEY, awsConfig.getSecretKey());
    }

    @Test
    public void test_CheckNoAwsErrors_NoAwsErrors()
            throws Exception {
        // given
        int httpResponseCode = 200;

        HttpURLConnection httpConnection = mock(HttpURLConnection.class);
        given(httpConnection.getResponseCode()).willReturn(httpResponseCode);

        AwsConfig awsConfig = predefinedAwsConfigBuilder().build();
        DescribeInstances describeInstances = new DescribeInstances(awsConfig);

        // when
        describeInstances.checkNoAwsErrors(httpConnection);

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
        DescribeInstances describeInstances = new DescribeInstances(awsConfig);

        // when & then
        try {
            describeInstances.checkNoAwsErrors(httpConnection);
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
        DescribeInstances describeInstances = new DescribeInstances(awsConfig);

        // when & then
        try {
            describeInstances.checkNoAwsErrors(httpConnection);
            fail("AwsConnectionFailed exception was not thrown");
        } catch (AwsConnectionException e) {
            assertEquals(httpResponseCode, e.getHttpReponseCode());
            assertEquals("", e.getErrorMessage());
            assertThat(e.getMessage(), containsString(Integer.toString(httpResponseCode)));
        }
    }

    @Test
    public void test_DescribeInstances()
            throws Exception {
        // given
        AwsConfig awsConfig = predefinedAwsConfigBuilder().setAccessKey(System.getenv("AWS_ACCESS_KEY_ID"))
                                                          .setSecretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
                                                          .setSecurityGroupName("launch-wizard-147").build();

        DescribeInstances describeInstances = spy(new DescribeInstances(awsConfig, awsConfig.getHostHeader()));
        doReturn(stubDescribeInstancesResponse()).when(describeInstances).callService(HOST_HEADER);

        // when
        Map<String, String> result = describeInstances.execute();

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

    private static InputStream toInputStream(String s)
            throws Exception {
        return new ByteArrayInputStream(s.getBytes("UTF-8"));
    }

    private static AwsConfig.Builder predefinedAwsConfigBuilder() {
        return AwsConfig.builder().setHostHeader(HOST_HEADER).setRegion("us-east-1").setConnectionTimeoutSeconds(5);
    }
}
