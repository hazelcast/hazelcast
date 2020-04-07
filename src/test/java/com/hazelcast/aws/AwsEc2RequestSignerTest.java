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

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.aws.AwsEc2RequestSigner.SIGNATURE_METHOD_V4;
import static org.junit.Assert.assertEquals;

public class AwsEc2RequestSignerTest {
    private final static String REQUEST_DATE = "20141106T111126Z";

    private final AwsEc2RequestSigner requestSigner = new AwsEc2RequestSigner();

    @Test
    public void sign() {
        // given
        AwsCredentials credentials = AwsCredentials.builder()
            .setAccessKey("AKIDEXAMPLE")
            .setSecretKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
            .build();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("X-Amz-Date", REQUEST_DATE);
        attributes.put("Action", "DescribeInstances");
        attributes.put("Version", "2016-11-15");
        attributes.put("X-Amz-Algorithm", SIGNATURE_METHOD_V4);
        attributes.put("X-Amz-SignedHeaders", "host");
        attributes.put("X-Amz-Expires", "30");
        attributes.put("Filter.1.Value.1", "running");
        attributes.put("Filter.1.Name", "instance-state-name");
        attributes.put("X-Amz-Credential", "AKIDEXAMPLE/20141106/eu-central-1/ec2/aws4_request");

        // when
        String result = requestSigner.sign(attributes, "eu-central-1", "ec2.eu-central-1.amazonaws.com",
            credentials, REQUEST_DATE);

        // then
        assertEquals("79f7a4d346ee69ca22ba5f9bc3dd1efc13ac7509936afc5ec21cac37de071eef", result);
    }
}
