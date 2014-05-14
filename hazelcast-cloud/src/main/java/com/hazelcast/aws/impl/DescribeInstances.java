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

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.hazelcast.aws.impl.Constants.*;
import static com.hazelcast.aws.utility.CloudyUtility.unmarshalTheResponse;

public class DescribeInstances {

    private final EC2RequestSigner rs;
    private final AwsConfig awsConfig;

    public DescribeInstances(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        if (awsConfig.getAccessKey() == null) {
            throw new IllegalArgumentException("AWS access key is required!");
        }
        rs = new EC2RequestSigner(awsConfig.getSecretKey());
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);
        attributes.put("SignatureVersion", SIGNATURE_VERSION);
        attributes.put("SignatureMethod", SIGNATURE_METHOD);
        attributes.put("AWSAccessKeyId", awsConfig.getAccessKey());
        attributes.put("Timestamp", getFormattedTimestamp());
        this.awsConfig = awsConfig;
    }

    /**
     * Formats date as ISO 8601 timestamp
     */
    private String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(new Date());
    }

    private Map<String, String> attributes = new HashMap<String, String>();

    public String getQueryString() {
        return CloudyUtility.getQueryString(attributes);
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void putSignature(String value) {
        attributes.put("Signature", value);
    }

    public <T> T execute(String endpoint) throws Exception {
        rs.sign(this, endpoint);
        Object result = callService(endpoint);
        return (T) result;
    }

    public Object callService(String endpoint) throws Exception {
        String query = getQueryString();
        URL url = new URL("https", endpoint, -1, "/" + query);
        HttpURLConnection httpConnection = (HttpURLConnection) (url.openConnection());
        httpConnection.setRequestMethod(GET);
        httpConnection.setDoOutput(true);
        httpConnection.connect();
        Object response = unmarshalTheResponse(httpConnection.getInputStream(), awsConfig);
        return response;
    }
}
