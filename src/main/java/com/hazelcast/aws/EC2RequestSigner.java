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

import com.hazelcast.internal.util.QuickMath;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

class EC2RequestSigner {

    private static final String NEW_LINE = "\n";
    private static final String API_TERMINATOR = "aws4_request";
    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final String UTF_8 = "UTF-8";
    private static final int DATE_LENGTH = 8;
    private static final int LAST_INDEX = 8;

    private final String timestamp;

    private String service;
    private Map<String, String> attributes;
    private final String region;
    private final String endpoint;
    private final AwsCredentials credentials;

    EC2RequestSigner(String timestamp, String region, String endpoint, AwsCredentials credentials) {
        this.timestamp = timestamp;
        this.region = region;
        this.endpoint = endpoint;
        this.credentials = credentials;
    }

    private String getCredentialScope() {
        // datestamp/region/service/API_TERMINATOR
        String dateStamp = timestamp.substring(0, DATE_LENGTH);
        return format("%s/%s/%s/%s", dateStamp, region, this.service, API_TERMINATOR);
    }

    private String getSignedHeaders() {
        return "host";
    }

    String sign(String service, Map<String, String> attributes) {
        this.service = service;
        this.attributes = attributes;

        String canonicalRequest = getCanonicalizedRequest();
        String stringToSign = createStringToSign(canonicalRequest);
        byte[] signingKey = deriveSigningKey();

        return createSignature(stringToSign, signingKey);
    }

    /* Task 1 */
    private String getCanonicalizedRequest() {
        return Constants.GET + NEW_LINE + '/' + NEW_LINE + getCanonicalizedQueryString(this.attributes) + NEW_LINE
            + getCanonicalHeaders() + NEW_LINE + getSignedHeaders() + NEW_LINE + sha256Hashhex("");
    }

    /* Task 2 */
    private String createStringToSign(String canonicalRequest) {
        return Constants.SIGNATURE_METHOD_V4 + NEW_LINE + timestamp + NEW_LINE + getCredentialScope() + NEW_LINE + sha256Hashhex(
            canonicalRequest);
    }

    /* Task 3 */
    private byte[] deriveSigningKey() {
        String signKey = credentials.getSecretKey();
        String dateStamp = timestamp.substring(0, DATE_LENGTH);
        // this is derived from
        // http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python

        try {
            String key = "AWS4" + signKey;
            Mac mDate = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skDate = new SecretKeySpec(key.getBytes(UTF_8), HMAC_SHA256);
            mDate.init(skDate);
            byte[] kDate = mDate.doFinal(dateStamp.getBytes(UTF_8));

            Mac mRegion = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skRegion = new SecretKeySpec(kDate, HMAC_SHA256);
            mRegion.init(skRegion);
            byte[] kRegion = mRegion.doFinal(region.getBytes(UTF_8));

            Mac mService = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skService = new SecretKeySpec(kRegion, HMAC_SHA256);
            mService.init(skService);
            byte[] kService = mService.doFinal(this.service.getBytes(UTF_8));

            Mac mSigning = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skSigning = new SecretKeySpec(kService, HMAC_SHA256);
            mSigning.init(skSigning);

            return mSigning.doFinal("aws4_request".getBytes(UTF_8));
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (InvalidKeyException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    private String createSignature(String stringToSign, byte[] signingKey) {
        byte[] signature;
        try {
            Mac signMac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec signKS = new SecretKeySpec(signingKey, HMAC_SHA256);
            signMac.init(signKS);
            signature = signMac.doFinal(stringToSign.getBytes(UTF_8));
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (InvalidKeyException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
        return QuickMath.bytesToHex(signature);
    }

    private String getCanonicalHeaders() {
        return format("host:%s%s", endpoint, NEW_LINE);
    }

    String getCanonicalizedQueryString(Map<String, String> attributes) {
        List<String> components = getListOfEntries(attributes);
        Collections.sort(components);
        return getCanonicalizedQueryString(components);
    }

    private String getCanonicalizedQueryString(List<String> list) {
        Iterator<String> it = list.iterator();
        StringBuilder result = new StringBuilder(it.next());
        while (it.hasNext()) {
            result.append('&').append(it.next());
        }
        return result.toString();
    }

    private void addComponents(List<String> components, Map<String, String> attributes, String key) {
        components.add(AwsURLEncoder.urlEncode(key) + '=' + AwsURLEncoder.urlEncode(attributes.get(key)));
    }

    private List<String> getListOfEntries(Map<String, String> entries) {
        List<String> components = new ArrayList<String>();
        for (String key : entries.keySet()) {
            addComponents(components, entries, key);
        }
        return components;
    }

    private String sha256Hashhex(String in) {
        String payloadHash;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(in.getBytes(UTF_8));
            byte[] digest = md.digest();
            payloadHash = QuickMath.bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
        return payloadHash;
    }

    String createFormattedCredential() {
        return credentials.getAccessKey() + '/' + timestamp.substring(0, LAST_INDEX) + '/' + region + '/'
            + "ec2/aws4_request";
    }
}
