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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

public class AwsRequestSignerTest {

    @Test
    public void authHeaderEc2() {
        // given
        String timestamp = "20141106T111126Z";

        Map<String, String> attributes = new HashMap<>();
        attributes.put("Action", "DescribeInstances");
        attributes.put("Version", "2016-11-15");

        Map<String, String> headers = new HashMap<>();
        headers.put("X-Amz-Date", timestamp);
        headers.put("Host", "ec2.eu-central-1.amazonaws.com");

        String body = "";

        AwsCredentials credentials = AwsCredentials.builder()
            .setAccessKey("AKIDEXAMPLE")
            .setSecretKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
            .build();

        AwsRequestSigner requestSigner = new AwsRequestSigner("eu-central-1", "ec2");

        // when
        String authHeader = requestSigner.authHeader(attributes, headers, body, credentials, timestamp, "POST");

        // then
        String expectedAuthHeader = "AWS4-HMAC-SHA256 "
            + "Credential=AKIDEXAMPLE/20141106/eu-central-1/ec2/aws4_request, "
            + "SignedHeaders=host;x-amz-date, "
            + "Signature=cedc903f54260b232ced76caf4a72f061565a51cc583a17da87b1132522f5893";
        assertEquals(expectedAuthHeader, authHeader);
    }

    @Test
    public void authHeaderEcs() {
        // given
        String timestamp = "20141106T111126Z";

        Map<String, String> headers = new HashMap<>();
        headers.put("X-Amz-Date", timestamp);
        headers.put("Host", "ecs.eu-central-1.amazonaws.com");

        //language=JSON
        String body = "{\n"
            + "  \"cluster\": \"123456\",\n"
            + "  \"family\": \"abcdef\"\n"
            + "}";

        AwsCredentials credentials = AwsCredentials.builder()
            .setAccessKey("AKIDEXAMPLE")
            .setSecretKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
            .build();

        AwsRequestSigner requestSigner = new AwsRequestSigner("eu-central-1", "ecs");

        // when
        String authHeader = requestSigner.authHeader(emptyMap(), headers, body, credentials, timestamp, "GET");

        // then
        String expectedAuthHeader = "AWS4-HMAC-SHA256 "
            + "Credential=AKIDEXAMPLE/20141106/eu-central-1/ecs/aws4_request, "
            + "SignedHeaders=host;x-amz-date, "
            + "Signature=d25323cd86f9e960d0303599891d54fb9a1a0975bd132c06e95f767118d5bf55";
        assertEquals(expectedAuthHeader, authHeader);
    }
}
