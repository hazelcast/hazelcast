/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.gcp;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.core.HazelcastException;

import java.util.Base64;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;

/**
 * Fetches OAuth 2.0 Access Token from Google API.
 *
 * @see <a href="https://developers.google.com/identity/protocols/OAuth2ServiceAccount">Using OAuth 2.0 for Server to Server</a>
 */
class GcpAuthenticator {
    private static final String GOOGLE_AUTH_ENDPOINT = "https://www.googleapis.com/oauth2/v4/token";
    private static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    private final String endpoint;

    GcpAuthenticator() {
        this(GOOGLE_AUTH_ENDPOINT);
    }

    /**
     * For test purposes only.
     */
    GcpAuthenticator(String endpoint) {
        this.endpoint = endpoint;
    }

    String refreshAccessToken(String privateKeyPath) {
        return refreshAccessToken(privateKeyPath, System.currentTimeMillis());
    }

    String refreshAccessToken(String privateKeyPath, long currentTimeMs) {
        try {
            String body = createBody(privateKeyPath, currentTimeMs);
            String response = callService(body);
            return parseResponse(response);
        } catch (Exception e) {
            throw new HazelcastException("Error while fetching access token from Google API", e);
        }
    }

    private String createBody(String privateKeyPath, long currentTimeMs)
            throws Exception {
        JsonObject privateKeyJson = Json.parse(new InputStreamReader(new FileInputStream(privateKeyPath), "UTF-8")).asObject();
        String privateKey = privateKeyJson.get("private_key").asString();
        String clientEmail = privateKeyJson.get("client_email").asString();

        String headerBase64 = base64encodeUrlSafe(header());
        String claimSetBase64 = base64encodeUrlSafe(claimSet(clientEmail, currentTimeMs));
        String signatureBase64 = sign(headerBase64, claimSetBase64, privateKey);

        return body(headerBase64, claimSetBase64, signatureBase64);
    }

    private static String header() {
        return "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private String claimSet(String clientEmail, long currentTimeMs) {
        long currentTimeSeconds = currentTimeMs / 1000L;
        long expirationTimeSeconds = currentTimeSeconds + 3600L;
        return String.format("{\"iss\":\"%s\",\"scope\":\"%s\",\"aud\":\"%s\",\"iat\":%s,\"exp\":%s}", clientEmail, SCOPE,
                endpoint, currentTimeSeconds, expirationTimeSeconds);
    }

    private static String body(String headerBase64, String claimSetBase64, String signatureBase64) {
        String grantType = "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer";
        String assertion = String.format("%s.%s.%s", headerBase64, claimSetBase64, signatureBase64);
        return String.format("grant_type=%s&assertion=%s", grantType, assertion);
    }

    private static String sign(String headerBase64, String claimSetBase64, String privateKeyString)
            throws Exception {
        String dataToSign = String.format("%s.%s", headerBase64, claimSetBase64);

        String clearPrivateKeyString = clear(privateKeyString);
        byte[] b1 = Base64.getMimeDecoder().decode(clearPrivateKeyString.getBytes("UTF-8"));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(b1);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        Signature privateSignature = Signature.getInstance("SHA256withRSA");
        PrivateKey privateKey = kf.generatePrivate(spec);

        privateSignature.initSign(privateKey);
        privateSignature.update(dataToSign.getBytes("UTF-8"));
        return new String(base64encodeUrlSafe(privateSignature.sign()), "UTF-8");
    }

    private static String clear(String privateKey) {
        return privateKey.replaceAll("-----END PRIVATE KEY-----", "")
                         .replaceAll("-----BEGIN PRIVATE KEY-----", "")
                         .replaceAll("\\\\n", "");
    }

    private static String base64encodeUrlSafe(String data)
            throws UnsupportedEncodingException {
        byte[] encoded = base64encodeUrlSafe(data.getBytes("UTF-8"));
        return new String(encoded, "UTF-8");
    }

    private static byte[] base64encodeUrlSafe(byte[] data) {
        byte[] encode = Base64.getEncoder().encode(data);
        for (int i = 0; i < encode.length; i++) {
            if (encode[i] == '+') {
                encode[i] = '-';
            } else if (encode[i] == '/') {
                encode[i] = '_';
            }
        }
        return encode;
    }

    private String callService(String body) {
        return RestClient.create(endpoint).withBody(body).post();
    }

    private static String parseResponse(String response) {
        return Json.parse(response).asObject().get("access_token").asString();
    }
}
