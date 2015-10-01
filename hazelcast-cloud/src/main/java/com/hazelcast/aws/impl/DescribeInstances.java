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
import com.hazelcast.config.InvalidConfigurationException;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.aws.impl.Constants.DOC_VERSION;
import static com.hazelcast.aws.impl.Constants.SIGNATURE_METHOD_V4;
import static com.hazelcast.nio.IOUtil.closeResource;

public class DescribeInstances {
    private static final String IAM_ROLE_ENDPOINT = "169.254.169.254";
    String timeStamp = getFormattedTimestamp();
    private EC2RequestSigner rs;
    private AwsConfig awsConfig;
    private String endpoint;
    private Map<String, String> attributes = new HashMap<String, String>();

    public DescribeInstances(AwsConfig awsConfig, String endpoint) {
        if (awsConfig == null) {
            throw new IllegalArgumentException("AwsConfig is required!");
        }
        if (awsConfig.getAccessKey() == null && awsConfig.getIamRole() == null) {
            throw new IllegalArgumentException("AWS access key or IAM Role is required!");
        }
        this.awsConfig = awsConfig;
        this.endpoint = endpoint;
        if (awsConfig.getIamRole() != null) {
            getKeysFromIamRole();
        }
        rs = new EC2RequestSigner(awsConfig, timeStamp, endpoint);
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);
        attributes.put("X-Amz-Algorithm", SIGNATURE_METHOD_V4);
        attributes.put("X-Amz-Credential", rs.createFormattedCredential());
        attributes.put("X-Amz-Date", timeStamp);
        attributes.put("X-Amz-SignedHeaders", "host");
        attributes.put("X-Amz-Expires", "30");
    }

    private void getKeysFromIamRole() {
        try {
            String query = "latest/meta-data/iam/security-credentials/" + awsConfig.getIamRole();
            URL url = new URL("http", IAM_ROLE_ENDPOINT, query);
            InputStreamReader is = new InputStreamReader(url.openStream(), "UTF-8");
            BufferedReader reader = new BufferedReader(is);
            Map<String, String> map = parseIamRole(reader);
            awsConfig.setAccessKey(map.get("AccessKeyId"));
            awsConfig.setSecretKey(map.get("SecretAccessKey"));
            attributes.put("X-Amz-Security-Token", map.get("Token"));
        } catch (IOException io) {
            throw new InvalidConfigurationException("Invalid Aws Configuration");
        }
    }

    public Map parseIamRole(BufferedReader reader) throws IOException {
        Map map = new HashMap();
        Pattern keyPattern = Pattern.compile("\"(.*?)\" : ");
        Pattern valuePattern = Pattern.compile(" : \"(.*?)\",");
        String line;
        for (line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.contains(":")) {
                Matcher keyMatcher = keyPattern.matcher(line);
                Matcher valueMatcher = valuePattern.matcher(line);
                if (keyMatcher.find() && valueMatcher.find()) {
                    String key = keyMatcher.group(1);
                    String value = valueMatcher.group(1);
                    map.put(key, value);
                }
            }
        }
        return map;
    }

    private String getFormattedTimestamp() {
        SimpleDateFormat df = new SimpleDateFormat(Constants.DATE_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        return df.format(new Date());
    }

    public Map<String, String> execute() throws Exception {
        final String signature = rs.sign("ec2", attributes);
        Map response = null;
        InputStream stream = null;
        attributes.put("X-Amz-Signature", signature);
        try {
            stream = callService(endpoint, signature);
            response = CloudyUtility.unmarshalTheResponse(stream, awsConfig);
            return response;
        } finally {
            closeResource(stream);
        }
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
