/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.aws.impl;

import com.hazelcast.aws.security.EC2RequestSigner;
import com.hazelcast.aws.utility.CloudyUtility;

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.aws.impl.Constants.*;
import static com.hazelcast.aws.utility.CloudyUtility.unmarshalTheResponse;

public class DescribeInstances {
    private static final int FIVE_MINUTES = 5 * 60 * 1000;
    private final EC2RequestSigner rs;

    public DescribeInstances(String accessKey, String secretKey) {
        rs = new EC2RequestSigner(secretKey);
        attributes.put("Action", this.getClass().getSimpleName());
        attributes.put("Version", DOC_VERSION);
        attributes.put("SignatureVersion", SIGNATURE_VERSION);
        attributes.put("SignatureMethod", SIGNATURE_METHOD);
        attributes.put("AWSAccessKeyId", accessKey);
        attributes.put("Expires", new SimpleDateFormat(DATE_FORMAT).format(new Date(GregorianCalendar.getInstance().getTimeInMillis() + FIVE_MINUTES)));
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

    public <T> T execute() throws Exception {
        rs.sign(this);
        Object result = callService();
        return (T) result;
    }

    public Object callService() throws Exception {
        String query = getQueryString();
        URL url = new URL("https", HOST_HEADER, -1, "/" + query);
        HttpURLConnection httpConnection = (HttpURLConnection) (url.openConnection());
        httpConnection.setRequestMethod(GET);
        httpConnection.setDoOutput(true);
        httpConnection.connect();
        if (httpConnection.getResponseCode() != HttpURLConnection.HTTP_OK)
            throw new Exception("Https response code is: " + httpConnection.getResponseCode());
        Object response = unmarshalTheResponse(httpConnection.getInputStream());
        return response;
    }
}
