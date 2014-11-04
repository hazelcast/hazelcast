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

package com.hazelcast.aws.impl;

import com.hazelcast.aws.security.EC2RequestSigner;
import com.hazelcast.aws.utility.CloudyUtility;
import com.hazelcast.config.AwsConfig;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.aws.impl.Constants.*;

public class DescribeInstances {

    private final EC2RequestSigner rs;
    private final AwsConfig awsConfig;

    private Map<String, String> attributes = new HashMap<String, String>();

    public DescribeInstances(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        if (awsConfig.getAccessKey() == null) {
            throw new IllegalArgumentException("AWS access key is required!");
        }

        rs = new EC2RequestSigner();
        attributes.put("SignatureMethod", SIGNATURE_METHOD_V4);
        attributes.put("X-Amz-Date", rs.getFormattedTimestamp());
        attributes.put("Region", awsConfig.getRegion());
        attributes.put("Service", "ec2");
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);

        this.awsConfig = awsConfig;
    }

    public String getQueryString() {
        return CloudyUtility.getQueryString(attributes);
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Map<String, String> execute(String endpoint) throws Exception {
        final String authorization = rs.sign(awsConfig, this, endpoint);
        InputStream stream = callService(endpoint, authorization);
        return CloudyUtility.unmarshalTheResponse(stream, awsConfig);
    }

    private InputStream callService(String endpoint, String authorization) throws Exception {
        String query = getQueryString();
        URL url = new URL("https", endpoint, -1, "/" + query);
        HttpURLConnection httpConnection = (HttpURLConnection) (url.openConnection());
        httpConnection.setRequestMethod(GET);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty("Authorization", authorization);
        httpConnection.connect();
        return httpConnection.getInputStream();
    }
}
