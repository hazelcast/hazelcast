/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for hashing with MD5
 */
public final class Sha256Util {

    private Sha256Util() {
    }

    /**
     * Calculate the SHA256 of given file
     *
     * @param jarPath specifies the path to file
     * @return SHA256 as hexadecimal string
     * @throws IOException              in case of IO error
     * @throws NoSuchAlgorithmException in case of MessageDigest error
     */
    public static String calculateSha256Hex(Path jarPath) throws IOException, NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        try (InputStream inputStream = Files.newInputStream(jarPath);
             DigestInputStream digestInputStream = new DigestInputStream(inputStream, messageDigest)) {

            final int oneMB = 1024 * 1024;
            byte[] buffer = new byte[oneMB];
            int readCount;
            do {
                readCount = digestInputStream.read(buffer);
            } while (readCount >= 0);
        }
        BigInteger bigInteger = new BigInteger(1, messageDigest.digest());
        final int radix = 16;
        return bigInteger.toString(radix);

    }
}
