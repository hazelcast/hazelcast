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

package com.hazelcast.internal.util;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for hashing with MD5
 */
public final class MD5Util {

    private MD5Util() {
    }

    /**
     * Converts given string to MD5 hash
     *
     * @param str str to be hashed with MD5
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static String toMD5String(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            if (md == null || str == null) {
                return null;
            }
            byte[] byteData = md.digest(str.getBytes(StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            for (byte aByteData : byteData) {
                sb.append(Integer.toString((aByteData & 0xff) + 0x100, 16).substring(1));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException ignored) {
            return null;
        }
    }

    /**
     * Calculate the MD5 of given file
     * @param jarPath specifies the path to file
     * @return MD5 as hexadecimal string
     * @throws IOException in case of IO error
     * @throws NoSuchAlgorithmException in case of MessageDigest error
     */
    public static String calculateMd5Hex(Path jarPath) throws IOException, NoSuchAlgorithmException {
        try (ReadableByteChannel in = Channels.newChannel(Files.newInputStream(jarPath))) {
            MessageDigest md5Digest = MessageDigest.getInstance("MD5");

            // 1 MB
            final int oneMB = 1024 * 1024;
            ByteBuffer buffer = ByteBuffer.allocate(oneMB);

            while (in.read(buffer) != -1) {
                buffer.flip();
                md5Digest.update(buffer.asReadOnlyBuffer());
                buffer.clear();
            }

            BigInteger md5Actual = new BigInteger(1, md5Digest.digest());
            final int radix = 16;
            return md5Actual.toString(radix);
        }
    }
}
