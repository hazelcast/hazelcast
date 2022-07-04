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

import com.hazelcast.internal.util.QuickMath;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

import static com.hazelcast.aws.AwsRequestUtils.canonicalQueryString;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Responsible for signing AWS Requests with the Signature version 4.
 * <p>
 * The signing steps are described in the AWS Documentation.
 *
 * @see <a href="https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html">Signature Version 4 Signing Process</a>
 */
class AwsRequestSigner {
    private static final String SIGNATURE_METHOD_V4 = "AWS4-HMAC-SHA256";
    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final int TIMESTAMP_FIELD_LENGTH = 8;

    private final String region;
    private final String service;

    AwsRequestSigner(String region, String service) {
        this.region = region;
        this.service = service;
    }

    String authHeader(Map<String, String> attributes, Map<String, String> headers, String body,
                      AwsCredentials credentials, String timestamp, String httpMethod) {
        return buildAuthHeader(
            credentials.getAccessKey(),
            credentialScopeEcs(timestamp),
            signedHeaders(headers),
            sign(attributes, headers, body, credentials, timestamp, httpMethod)
        );
    }

    private String buildAuthHeader(String accessKey, String credentialScope, String signedHeaders, String signature) {
        return String.format("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
            SIGNATURE_METHOD_V4, accessKey, credentialScope, signedHeaders, signature);
    }

    private String credentialScopeEcs(String timestamp) {
        // datestamp/region/service/API_TERMINATOR
        return format("%s/%s/%s/%s", datestamp(timestamp), region, service, "aws4_request");
    }

    private String sign(Map<String, String> attributes, Map<String, String> headers, String body,
                        AwsCredentials credentials, String timestamp, String httpMethod) {
        String canonicalRequest = canonicalRequest(attributes, headers, body, httpMethod);
        String stringToSign = stringToSign(canonicalRequest, timestamp);
        byte[] signingKey = signingKey(credentials, timestamp);
        return createSignature(stringToSign, signingKey);
    }

    /* Task 1 */
    private String canonicalRequest(Map<String, String> attributes, Map<String, String> headers,
                                    String body, String httpMethod) {
        return String.format("%s\n/\n%s\n%s\n%s\n%s",
            httpMethod,
            canonicalQueryString(attributes),
            canonicalHeaders(headers),
            signedHeaders(headers),
            sha256Hashhex(body)
        );
    }

    private String canonicalHeaders(Map<String, String> headers) {
        StringBuilder canonical = new StringBuilder();
        for (Map.Entry<String, String> entry : sortedLowercase(headers).entrySet()) {
            canonical.append(format("%s:%s\n", entry.getKey(), entry.getValue()));
        }
        return canonical.toString();
    }

    /* Task 2 */
    private String stringToSign(String canonicalRequest, String timestamp) {
        return String.format("%s\n%s\n%s\n%s",
            SIGNATURE_METHOD_V4,
            timestamp,
            credentialScope(timestamp),
            sha256Hashhex(canonicalRequest)
        );
    }

    private String credentialScope(String timestamp) {
        // datestamp/region/service/API_TERMINATOR
        return format("%s/%s/%s/%s", datestamp(timestamp), region, service, "aws4_request");
    }

    /* Task 3 */
    private byte[] signingKey(AwsCredentials credentials, String timestamp) {
        String signKey = credentials.getSecretKey();
        // this is derived from
        // http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python

        try {
            String key = "AWS4" + signKey;
            Mac mDate = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skDate = new SecretKeySpec(key.getBytes(UTF_8), HMAC_SHA256);
            mDate.init(skDate);
            byte[] kDate = mDate.doFinal(datestamp(timestamp).getBytes(UTF_8));

            Mac mRegion = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skRegion = new SecretKeySpec(kDate, HMAC_SHA256);
            mRegion.init(skRegion);
            byte[] kRegion = mRegion.doFinal(region.getBytes(UTF_8));

            Mac mService = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skService = new SecretKeySpec(kRegion, HMAC_SHA256);
            mService.init(skService);
            byte[] kService = mService.doFinal(service.getBytes(UTF_8));

            Mac mSigning = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec skSigning = new SecretKeySpec(kService, HMAC_SHA256);
            mSigning.init(skSigning);

            return mSigning.doFinal("aws4_request".getBytes(UTF_8));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            return null;
        }
    }

    private String createSignature(String stringToSign, byte[] signingKey) {
        try {
            Mac signMac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec signKS = new SecretKeySpec(signingKey, HMAC_SHA256);
            signMac.init(signKS);
            byte[] signature = signMac.doFinal(stringToSign.getBytes(UTF_8));
            return QuickMath.bytesToHex(signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            return null;
        }
    }

    private static String datestamp(String timestamp) {
        return timestamp.substring(0, TIMESTAMP_FIELD_LENGTH);
    }

    private String signedHeaders(Map<String, String> headers) {
        return String.join(";", sortedLowercase(headers).keySet());
    }

    private Map<String, String> sortedLowercase(Map<String, String> headers) {
        Map<String, String> sortedHeaders = new TreeMap<>();
        for (Map.Entry<String, String> e : headers.entrySet()) {
            sortedHeaders.put(e.getKey().toLowerCase(), e.getValue());
        }
        return sortedHeaders;
    }

    private static String sha256Hashhex(String in) {
        String payloadHash;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(in.getBytes(UTF_8));
            byte[] digest = md.digest();
            payloadHash = QuickMath.bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
        return payloadHash;
    }
}
