/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AbstractBasicSymmetricEncryptionConfig;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.nio.Bits;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class BasicSymmetricCipherBuilder {
    protected static final int IV_LENGTH_CBC = 8;
    private static final int IV_LENGTH_AES = 16;

    protected final String algorithm;
    // 8-byte Salt
    protected final byte[] salt;
    protected final byte[] saltDigest;

    public BasicSymmetricCipherBuilder(AbstractBasicSymmetricEncryptionConfig<?> config) {
        this.algorithm = config.getAlgorithm();
        this.salt = createSalt(config.getSalt());
        this.saltDigest = createSaltDigest(salt);
    }

    public Cipher create(boolean encryptMode, byte[] keyBytes) {
        checkNotNull(keyBytes, "Key bytes cannot be null");
        try {
            CipherParams params = createCipherParams(keyBytes);
            if (params == null) {
                throw new UnsupportedOperationException("Encryption algorithm not supported: " + algorithm);
            }
            AlgorithmParameterSpec paramSpec = buildFinalAlgorithmParameterSpec(params.ivLength, params.paramSpec);
            int mode = encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE;
            Cipher cipher = Cipher.getInstance(algorithm);
            cipher.init(mode, params.key, paramSpec);
            return cipher;
        } catch (Throwable e) {
            throw new RuntimeException("Unable to create Cipher (encrypt mode: " + encryptMode + "): " + e.getMessage(), e);
        }
    }

    protected CipherParams createCipherParams(byte[] keyBytes) throws GeneralSecurityException {
        SecretKey key = null;
        // CBC mode requires IvParameter with 8 byte input
        int ivLength = IV_LENGTH_CBC;

        if (algorithm.startsWith("AES")) {
            ivLength = IV_LENGTH_AES;
            key = new SecretKeySpec(keyBytes, "AES");
        } else if (algorithm.startsWith("Blowfish")) {
            key = new SecretKeySpec(keyBytes, "Blowfish");
        } else if (algorithm.startsWith("DESede")) {
            // requires at least 192 bits (24 bytes)
            KeySpec keySpec = new DESedeKeySpec(keyBytes);
            key = SecretKeyFactory.getInstance("DESede").generateSecret(keySpec);
        } else if (algorithm.startsWith("DES")) {
            KeySpec keySpec = new DESKeySpec(keyBytes);
            key = SecretKeyFactory.getInstance("DES").generateSecret(keySpec);
        }
        if (key == null) {
            return null;
        }
        return new CipherParams(ivLength, key);
    }

    private AlgorithmParameterSpec buildFinalAlgorithmParameterSpec(int ivLength, AlgorithmParameterSpec paramSpec) {
        boolean isCBC = algorithm.contains("/CBC/");
        if (isCBC) {
            byte[] iv = (ivLength == IV_LENGTH_CBC) ? salt : saltDigest;
            paramSpec = new IvParameterSpec(iv);
        }
        return paramSpec;
    }

    private static byte[] createSalt(String saltStr) {
        long hash = 0;
        final int prime = 31;
        char[] chars = saltStr.toCharArray();
        for (char c : chars) {
            hash = prime * hash + c;
        }
        byte[] result = new byte[Bits.LONG_SIZE_IN_BYTES];
        EndiannessUtil.writeLongB(BYTE_ARRAY_ACCESS, result, 0, hash);
        return result;
    }

    private static byte[] createSaltDigest(byte[] salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(salt);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String findKeyAlgorithm(String algorithm) {
        if (algorithm.indexOf('/') != -1) {
            return algorithm.substring(0, algorithm.indexOf('/'));
        }
        return algorithm;
    }

    protected static class CipherParams {
        private int ivLength;
        private SecretKey key;
        private AlgorithmParameterSpec paramSpec;

        public CipherParams(int ivLength, SecretKey key) {
            this(ivLength, key, null);
        }

        public CipherParams(int ivLength, SecretKey key, AlgorithmParameterSpec paramSpec) {
            this.ivLength = ivLength;
            this.key = key;
            this.paramSpec = paramSpec;
        }
    }

}
