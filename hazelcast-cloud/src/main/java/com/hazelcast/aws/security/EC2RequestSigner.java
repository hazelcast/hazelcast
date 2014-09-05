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

package com.hazelcast.aws.security;

import com.hazelcast.aws.impl.DescribeInstances;
import com.hazelcast.aws.utility.AwsURLEncoder;

import java.security.SignatureException;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

public class EC2RequestSigner {
    private static final String HTTP_VERB = "GET\n";
    private static final String HTTP_REQUEST_URI = "/\n";
    private final String secretKey;

    public EC2RequestSigner(String secretKey) {
        if (secretKey == null) {
            throw new IllegalArgumentException("AWS secret key is required!");
        }
        this.secretKey = secretKey;
    }

    public void sign(DescribeInstances request, String endpoint) {
        String canonicalizedQueryString = getCanonicalizedQueryString(request);
        String stringToSign = HTTP_VERB + endpoint + "\n" + HTTP_REQUEST_URI + canonicalizedQueryString;
        String signature = signTheString(stringToSign);
        request.putSignature(signature);
    }

    private String signTheString(String stringToSign) {
        String signature = null;
        try {
            signature = RFC2104HMAC.calculateRFC2104HMAC(stringToSign, secretKey);
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        }
        return signature;
    }

    private String getCanonicalizedQueryString(DescribeInstances request) {
        List<String> components = getListOfEntries(request.getAttributes());
        Collections.sort(components);
        return getCanonicalizedQueryString(components);
    }

    private void addComponents(List<String> components, Map<String, String> attributes, String key) {
        components.add(AwsURLEncoder.urlEncode(key) + "=" + AwsURLEncoder.urlEncode(attributes.get(key)));
    }

    private List<String> getListOfEntries(Map<String, String> entries) {
        List<String> components = new ArrayList<String>();
        for (String key : entries.keySet()) {
            addComponents(components, entries, key);
        }
        return components;
    }

    /**
     * @param list
     * @return
     */
    private String getCanonicalizedQueryString(List<String> list) {
        Iterator<String> it = list.iterator();
        StringBuilder result = new StringBuilder(it.next());
        while (it.hasNext()) {
            result.append("&").append(it.next());
        }
        return result.toString();
    }
}
