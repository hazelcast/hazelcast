/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests whether or not the codecs are modified by hand
 * comparing the MD5 hash codes generated by the client protocol
 * to the hash code of the file content.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class CodecHashTest {
    private static final String CODEC_DIRECTORY = "src/main/java/com/hazelcast/client/impl/protocol/codec";
    private static final String CUSTOM_CODEC_DIRECTORY = "src/main/java/com/hazelcast/client/impl/protocol/codec/custom";
    private static final Pattern HASH_PATTERN = Pattern.compile("@Generated\\(\"([a-f0-9]{32})\"\\)");

    @Test
    public void testCodecHashCodes() throws IOException, NoSuchAlgorithmException {
        testCodecsInternal(CODEC_DIRECTORY);
    }

    @Test
    public void testCustomCodecHashCodes() throws IOException, NoSuchAlgorithmException {
        testCodecsInternal(CUSTOM_CODEC_DIRECTORY);
    }

    public void testCodecsInternal(String codecDirectory) throws IOException, NoSuchAlgorithmException {
        File codecDir = new File(codecDirectory);
        if (!codecDir.exists() || !codecDir.isDirectory()) {
            fail("Cannot find codecs in the " + codecDirectory);
        }
        for (String filename : Objects.requireNonNull(codecDir.list())) {
            File codecFile = new File(codecDir, filename);
            if (codecFile.isFile() && codecFile.getName().endsWith("Codec.java")) {
                try (BufferedReader br = new BufferedReader(new FileReader(codecFile))) {
                    StringBuilder builder = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) {
                        builder.append(line).append("\n");
                    }
                    String codecContent = builder.toString();
                    String hashCode = getHashCode(codecContent, filename);

                    assertEquals(generateErrorMessage(filename, "Hash code does not match with the file content."),
                            hashCode, calculateHashCode(codecContent));
                }
            }
        }
    }

    private String generateErrorMessage(String codecName, String cause) {
        return codecName + " modified by hand. "
                + "Instead, use hazelcast-client-protocol to generate this codec. Cause: "
                + cause;
    }

    private String getHashCode(String codecContent, String filename) {
        Matcher m = HASH_PATTERN.matcher(codecContent);
        boolean found = m.find();
        if (!found) {
            fail(generateErrorMessage(filename, "Cannot find a hash code in the file."));
        }
        return m.group(1);
    }

    private String calculateHashCode(String codecContent) throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("md5");
        byte[] digest = md5.digest(codecContent.replaceFirst(HASH_PATTERN.toString(),
                "@Generated(\"!codec_hash!\")").getBytes());
        return bytesToMD5HashString(digest);
    }

    private String bytesToMD5HashString(byte[] hash) {
        StringBuilder builder = new StringBuilder();
        for (byte b : hash) {
            builder.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
        }
        return builder.toString();
    }
}
