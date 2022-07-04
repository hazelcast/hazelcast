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

package com.hazelcast.aws;

import com.hazelcast.config.InvalidConfigurationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class AwsCredentialsProviderTest {
    private static final String ACCESS_KEY = "AKIDEXAMPLE";
    private static final String SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private static final String TOKEN = "IQoJb3JpZ2luX2VjEFIaDGV1LWNlbnRyYWwtMSJGM==";
    private static final AwsCredentials CREDENTIALS = AwsCredentials.builder()
        .setAccessKey(ACCESS_KEY)
        .setSecretKey(SECRET_KEY)
        .setToken(TOKEN)
        .build();

    @Mock
    private AwsMetadataApi awsMetadataApi;

    @Mock
    private Environment environment;

    @Test
    public void credentialsAccessKey() {
        // given
        AwsConfig awsConfig = AwsConfig.builder()
            .setAccessKey(ACCESS_KEY)
            .setSecretKey(SECRET_KEY)
            .build();
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider(awsConfig, awsMetadataApi, environment);

        // when
        AwsCredentials credentials = credentialsProvider.credentials();

        // then
        assertEquals(ACCESS_KEY, credentials.getAccessKey());
        assertEquals(SECRET_KEY, credentials.getSecretKey());
        assertNull(credentials.getToken());
    }

    @Test
    public void credentialsEc2IamRole() {
        // given
        String iamRole = "sample-iam-role";
        AwsConfig awsConfig = AwsConfig.builder()
            .setIamRole(iamRole)
            .build();
        given(awsMetadataApi.credentialsEc2(iamRole)).willReturn(CREDENTIALS);
        given(environment.isRunningOnEcs()).willReturn(false);
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider(awsConfig, awsMetadataApi, environment);

        // when
        AwsCredentials credentials = credentialsProvider.credentials();

        // then
        assertEquals(CREDENTIALS, credentials);
    }

    @Test
    public void credentialsDefaultEc2IamRole() {
        // given
        String iamRole = "sample-iam-role";
        AwsConfig awsConfig = AwsConfig.builder().build();
        given(awsMetadataApi.defaultIamRoleEc2()).willReturn(iamRole);
        given(awsMetadataApi.credentialsEc2(iamRole)).willReturn(CREDENTIALS);
        given(environment.isRunningOnEcs()).willReturn(false);
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider(awsConfig, awsMetadataApi, environment);

        // when
        AwsCredentials credentials = credentialsProvider.credentials();

        // then
        assertEquals(CREDENTIALS, credentials);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void credentialsEc2Exception() {
        // given
        String iamRole = "sample-iam-role";
        AwsConfig awsConfig = AwsConfig.builder()
            .setIamRole(iamRole)
            .build();
        given(awsMetadataApi.credentialsEc2(iamRole)).willThrow(new RuntimeException("Error fetching credentials"));
        given(environment.isRunningOnEcs()).willReturn(false);
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider(awsConfig, awsMetadataApi, environment);

        // when
        credentialsProvider.credentials();

        // then
        // throws exception
    }

    @Test
    public void credentialsEcs() {
        // given
        AwsConfig awsConfig = AwsConfig.builder().build();
        given(awsMetadataApi.credentialsEcs()).willReturn(CREDENTIALS);
        given(environment.isRunningOnEcs()).willReturn(true);
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider(awsConfig, awsMetadataApi, environment);

        // when
        AwsCredentials credentials = credentialsProvider.credentials();

        // then
        assertEquals(CREDENTIALS, credentials);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void credentialsEcsException() {
        // given
        AwsConfig awsConfig = AwsConfig.builder().build();
        given(awsMetadataApi.credentialsEcs()).willThrow(new RuntimeException("Error fetching credentials"));
        given(environment.isRunningOnEcs()).willReturn(true);
        AwsCredentialsProvider credentialsProvider = new AwsCredentialsProvider(awsConfig, awsMetadataApi, environment);

        // when
        credentialsProvider.credentials();

        // then
        // throws exception
    }
}
