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

package com.hazelcast.config.security;

import static com.hazelcast.config.ConfigXmlGenerator.MASK_FOR_SENSITIVE_DATA;
import static com.hazelcast.internal.util.StringUtil.trim;
import static java.nio.charset.StandardCharsets.US_ASCII;

import java.util.Base64;
import java.util.function.Function;

/**
 * Possible token encodings.
 */
public enum TokenEncoding {
    /**
     * No token encoding used (ASCII charset expected).
     */
    NONE("none", ba -> new String(ba, US_ASCII), s -> s.getBytes(US_ASCII)),
    /**
     * Base64 token encoding.
     */
    BASE64("base64", ba -> Base64.getEncoder().encodeToString(ba), s -> {
        if (MASK_FOR_SENSITIVE_DATA.equals(s)) {
            return MASK_FOR_SENSITIVE_DATA.getBytes(US_ASCII);
        } else {
            return Base64.getDecoder().decode(s);
        }
    });

    private static final TokenEncoding DEFAULT = TokenEncoding.NONE;

    private final String valueString;
    private final Function<byte[], String> encoder;
    private final Function<String, byte[]> decoder;

    TokenEncoding(String valueString, Function<byte[], String> encoder, Function<String, byte[]> decoder) {
        this.valueString = valueString;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public String toString() {
        return valueString;
    }

    public String encode(byte[] token) {
        return token == null ? null : encoder.apply(token);
    }

    public byte[] decode(String str) {
        return str == null ? null : decoder.apply(str);
    }

    public static TokenEncoding getTokenEncoding(String label) {
        label = trim(label);
        if (label == null) {
            return DEFAULT;
        }
        for (TokenEncoding item : TokenEncoding.values()) {
            if (item.toString().equals(label)) {
                return item;
            }
        }
        return DEFAULT;
    }

    public static TokenEncoding getEncodingForBytes(byte[] token) {
        return isAsciiOnly(token) ? NONE : BASE64;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static boolean isAsciiOnly(byte[] token) {
        if (token == null) {
            return true;
        }
        for (byte b : token) {
            int num = b & 0xff;
            if (num < 32 || num >= 127) {
                return false;
            }
        }
        return true;
    }
}
