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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.SignatureException;

import static com.hazelcast.aws.impl.Constants.SIGNATURE_METHOD;
import static com.hazelcast.util.Base64.encode;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

public final class RFC2104HMAC {

    private RFC2104HMAC() {
    }

    public static String calculateRFC2104HMAC(String data, String key)
            throws SignatureException {
        String result;
        try {
            SecretKeySpec signingKey = new SecretKeySpec(stringToBytes(key),
                    SIGNATURE_METHOD);
            Mac mac = Mac.getInstance(SIGNATURE_METHOD);
            mac.init(signingKey);
            byte[] rawSignature = mac.doFinal(stringToBytes(data));
            result = bytesToString(encode(rawSignature));
            result = result.trim();
        } catch (Exception e) {
            throw new SignatureException("Failed to generate HMAC : "
                    + e.getMessage());
        }
        return result;
    }
}
