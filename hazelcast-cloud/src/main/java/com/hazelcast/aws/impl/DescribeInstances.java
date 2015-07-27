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
import com.hazelcast.nio.IOUtil;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.hazelcast.aws.impl.Constants.DOC_VERSION;

public class DescribeInstances {
    private EC2RequestSigner rs;
    private AwsConfig awsConfig;
    private String timeStamp;

    private Map<String, String> attributes = new HashMap<String, String>();

    public DescribeInstances(AwsConfig awsConfig) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        if (awsConfig.getAccessKey() == null) {
            throw new IllegalArgumentException("AWS access key is required!");
        }
        this.awsConfig = awsConfig;
        this.timeStamp = getFormattedTimestamp();

        rs = new EC2RequestSigner(awsConfig, this.timeStamp);
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);
    }

    private String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.DATE_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(new Date());
    }

    public Map<String, String> execute() throws Exception {
        final String endpoint = String.format("ec2.%s.amazonaws.com", awsConfig.getRegion());
        final String authHeader = rs.createAuthHeader("ec2", attributes);
        InputStream stream = callService(endpoint, authHeader);
        return CloudyUtility.unmarshalTheResponse(stream, awsConfig);
    }

    private InputStream callService(String endpoint, String authHeader) throws Exception {
        String query = rs.getCanonicalizedQueryString(attributes);
        URL url = new URL("https", endpoint, -1, "/?" + query);
        HttpURLConnection httpConnection = (HttpURLConnection) (url.openConnection());
        httpConnection.setRequestMethod(Constants.GET);
        httpConnection.setDoOutput(true);
        httpConnection.setRequestProperty("Authorization", authHeader);
        httpConnection.setRequestProperty("x-amz-date", this.timeStamp);
        httpConnection.connect();
        return httpConnection.getInputStream();
    }
}
