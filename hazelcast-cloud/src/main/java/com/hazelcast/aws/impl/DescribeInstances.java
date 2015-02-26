/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.hazelcast.aws.impl.Constants.DOC_VERSION;
import static com.hazelcast.aws.impl.Constants.SIGNATURE_METHOD_V4;

public class DescribeInstances {
    String timeStamp = getFormattedTimestamp();
    private EC2RequestSigner rs;
    private AwsConfig awsConfig;
    private Map<String, String> attributes = new HashMap<String, String>();

    public DescribeInstances(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        if (awsConfig.getAccessKey() == null) {
            throw new IllegalArgumentException("AWS access key is required!");
        }
        this.awsConfig = awsConfig;

        rs = new EC2RequestSigner(awsConfig, timeStamp);
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);
        attributes.put("X-Amz-Algorithm", SIGNATURE_METHOD_V4);
        attributes.put("X-Amz-Credential", rs.createFormattedCredential());
        attributes.put("X-Amz-Date", timeStamp);
        attributes.put("X-Amz-SignedHeaders", "host");
        attributes.put("X-Amz-Expires", "30");
    }

    private String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.DATE_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(new Date());
    }

    public Map<String, String> execute() throws Exception {
        final String endpoint = String.format(awsConfig.getHostHeader());
        final String signature = rs.sign("ec2", attributes);
        attributes.put("X-Amz-Signature", signature);
        InputStream stream = callService(endpoint, signature);
        return CloudyUtility.unmarshalTheResponse(stream, awsConfig);
    }

    private InputStream callService(String endpoint, String signature) throws Exception {
        String query = rs.getCanonicalizedQueryString(attributes);
        URL url = new URL("https", endpoint, -1, "/?" + query);
        HttpURLConnection httpConnection = (HttpURLConnection) (url.openConnection());
        httpConnection.setRequestMethod(Constants.GET);
        httpConnection.setDoOutput(false);
        httpConnection.connect();
        return httpConnection.getInputStream();
    }
}
