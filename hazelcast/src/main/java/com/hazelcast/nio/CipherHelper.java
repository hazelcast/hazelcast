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

package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.PBEKeySpec;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import static com.hazelcast.util.StringUtil.stringToBytes;

public final class CipherHelper {

    static final ILogger LOGGER = Logger.getLogger(CipherHelper.class);

    private static SymmetricCipherBuilder symmetricCipherBuilder;

    static {
        try {
            if (Boolean.getBoolean("hazelcast.security.bouncy.enabled")) {
                String provider = "org.bouncycastle.jce.provider.BouncyCastleProvider";
                Security.addProvider((Provider) Class.forName(provider).newInstance());
            }
        } catch (Exception e) {
            LOGGER.warning(e);
        }
    }

    private CipherHelper() {
    }

    @SuppressWarnings("SynchronizedMethod")
    public static synchronized Cipher createSymmetricReaderCipher(SymmetricEncryptionConfig config) throws Exception {
        if (symmetricCipherBuilder == null) {
            symmetricCipherBuilder = new SymmetricCipherBuilder(config);
        }
        return symmetricCipherBuilder.getReaderCipher();
    }

    @SuppressWarnings("SynchronizedMethod")
    public static synchronized Cipher createSymmetricWriterCipher(SymmetricEncryptionConfig config) throws Exception {
        if (symmetricCipherBuilder == null) {
            symmetricCipherBuilder = new SymmetricCipherBuilder(config);
        }
        return symmetricCipherBuilder.getWriterCipher();
    }

    public static boolean isSymmetricEncryptionEnabled(IOService ioService) {
        SymmetricEncryptionConfig sec = ioService.getSymmetricEncryptionConfig();
        return (sec != null && sec.isEnabled());
    }

    static class SymmetricCipherBuilder {
        final String algorithm;
        // 8-byte Salt
        final byte[] salt;
        final String passPhrase;
        final int iterationCount;
        byte[] keyBytes;

        SymmetricCipherBuilder(SymmetricEncryptionConfig sec) {
            algorithm = sec.getAlgorithm();
            passPhrase = sec.getPassword();
            salt = createSalt(sec.getSalt());
            iterationCount = sec.getIterationCount();
            keyBytes = sec.getKey();
        }

        byte[] createSalt(String saltStr) {
            long hash = 0;
            char[] chars = saltStr.toCharArray();
            for (char c : chars) {
                hash = 31 * hash + c;
            }
            byte[] theSalt = new byte[8];
            theSalt[0] = (byte) (hash >>> 56);
            theSalt[1] = (byte) (hash >>> 48);
            theSalt[2] = (byte) (hash >>> 40);
            theSalt[3] = (byte) (hash >>> 32);
            theSalt[4] = (byte) (hash >>> 24);
            theSalt[5] = (byte) (hash >>> 16);
            theSalt[6] = (byte) (hash >>> 8);
            theSalt[7] = (byte) (hash);
            return theSalt;
        }

        public Cipher create(boolean encryptMode) {
            try {
                int mode = (encryptMode) ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE;
                Cipher cipher = Cipher.getInstance(algorithm);
                String keyAlgorithm = algorithm;
                if (algorithm.indexOf('/') != -1) {
                    keyAlgorithm = algorithm.substring(0, algorithm.indexOf('/'));
                }
                // 32-bit digest key=pass+salt
                ByteBuffer bbPass = ByteBuffer.allocate(32);
                MessageDigest md = MessageDigest.getInstance("MD5");
                bbPass.put(md.digest(stringToBytes(passPhrase)));
                md.reset();
                byte[] saltDigest = md.digest(salt);
                bbPass.put(saltDigest);
                boolean isCBC = algorithm.contains("/CBC/");
                SecretKey key = null;
                //CBC mode requires IvParameter with 8 byte input
                int ivLength = 8;
                AlgorithmParameterSpec paramSpec = null;
                if (keyBytes == null) {
                    keyBytes = bbPass.array();
                }
                if (algorithm.startsWith("AES")) {
                    ivLength = 16;
                    key = new SecretKeySpec(keyBytes, "AES");
                } else if (algorithm.startsWith("Blowfish")) {
                    key = new SecretKeySpec(keyBytes, "Blowfish");
                } else if (algorithm.startsWith("DESede")) {
                    //requires at least 192 bits (24 bytes)
                    KeySpec keySpec = new DESedeKeySpec(keyBytes);
                    key = SecretKeyFactory.getInstance("DESede").generateSecret(keySpec);
                } else if (algorithm.startsWith("DES")) {
                    KeySpec keySpec = new DESKeySpec(keyBytes);
                    key = SecretKeyFactory.getInstance("DES").generateSecret(keySpec);
                } else if (algorithm.startsWith("PBEWith")) {
                    paramSpec = new PBEParameterSpec(salt, iterationCount);
                    KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), salt, iterationCount);
                    key = SecretKeyFactory.getInstance(keyAlgorithm).generateSecret(keySpec);
                }
                if (isCBC) {
                    byte[] iv = (ivLength == 8) ? salt : saltDigest;
                    paramSpec = new IvParameterSpec(iv);
                }
                cipher.init(mode, key, paramSpec);
                return cipher;
            } catch (Throwable e) {
                throw new RuntimeException("unable to create Cipher:" + e.getMessage(), e);
            }
        }

        public Cipher getWriterCipher() {
            return create(true);
        }

        public Cipher getReaderCipher() {
            return create(false);
        }
    }

    public static void handleCipherException(Exception e, Connection connection) {
        LOGGER.warning(e);
        connection.close();
    }
}
