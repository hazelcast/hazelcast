/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.config.AsymmetricEncryptionConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.logging.Level;

final class CipherHelper {
    private static AsymmetricCipherBuilder asymmetricCipherBuilder = null;
    private static SymmetricCipherBuilder symmetricCipherBuilder = null;

    final static ILogger logger = Logger.getLogger(CipherHelper.class.getName());

    static {
        try {
            if (Boolean.getBoolean("hazelcast.security.bouncy.enabled")) {
                String provider = "org.bouncycastle.jce.provider.BouncyCastleProvider";
                Security.addProvider((Provider) Class.forName(provider).newInstance());
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    @SuppressWarnings("SynchronizedMethod")
    public static synchronized Cipher createAsymmetricReaderCipher(IOService ioService, String remoteAlias) throws Exception {
        if (asymmetricCipherBuilder == null) {
            asymmetricCipherBuilder = new AsymmetricCipherBuilder(ioService);
        }
        return asymmetricCipherBuilder.getReaderCipher(remoteAlias);
    }

    @SuppressWarnings("SynchronizedMethod")
    public static synchronized Cipher createAsymmetricWriterCipher(IOService ioService) throws Exception {
        if (asymmetricCipherBuilder == null) {
            asymmetricCipherBuilder = new AsymmetricCipherBuilder(ioService);
        }
        return asymmetricCipherBuilder.getWriterCipher();
    }

    @SuppressWarnings("SynchronizedMethod")
    public static synchronized Cipher createSymmetricReaderCipher(IOService ioService) throws Exception {
        if (symmetricCipherBuilder == null) {
            symmetricCipherBuilder = new SymmetricCipherBuilder(ioService.getSymmetricEncryptionConfig());
        }
        return symmetricCipherBuilder.getReaderCipher(null);
    }

    @SuppressWarnings("SynchronizedMethod")
    public static synchronized Cipher createSymmetricWriterCipher(IOService ioService) throws Exception {
        if (symmetricCipherBuilder == null) {
            symmetricCipherBuilder = new SymmetricCipherBuilder(ioService.getSymmetricEncryptionConfig());
        }
        return symmetricCipherBuilder.getWriterCipher();
    }

    public static boolean isAsymmetricEncryptionEnabled(IOService ioService) {
        AsymmetricEncryptionConfig aec = ioService.getAsymmetricEncryptionConfig();
        return (aec != null && aec.isEnabled());
    }

    public static boolean isSymmetricEncryptionEnabled(IOService ioService) {
        SymmetricEncryptionConfig sec = ioService.getSymmetricEncryptionConfig();
        return (sec != null && sec.isEnabled());
    }

    public static String getKeyAlias(IOService ioService) {
        AsymmetricEncryptionConfig aec = ioService.getAsymmetricEncryptionConfig();
        return aec.getKeyAlias();
    }

    interface CipherBuilder {
        Cipher getWriterCipher() throws Exception;

        Cipher getReaderCipher(String param) throws Exception;

        boolean isAsymmetric();
    }

    static class AsymmetricCipherBuilder implements CipherBuilder {
        String algorithm = "RSA/NONE/PKCS1PADDING";
        KeyStore keyStore;
        private final IOService ioService;

        AsymmetricCipherBuilder(IOService ioService) {
            this.ioService = ioService;
            try {
                AsymmetricEncryptionConfig aec = ioService.getAsymmetricEncryptionConfig();
                algorithm = aec.getAlgorithm();
                keyStore = KeyStore.getInstance(aec.getStoreType());
                // get user password and file input stream
                char[] password = aec.getStorePassword().toCharArray();
                java.io.FileInputStream fis =
                        new java.io.FileInputStream(aec.getStorePath());
                keyStore.load(fis, password);
                fis.close();
            } catch (Exception e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }

        public Cipher getReaderCipher(String remoteAlias) throws Exception {
            java.security.cert.Certificate certificate = keyStore.getCertificate(remoteAlias);
            PublicKey publicKey = certificate.getPublicKey();
            Cipher cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.DECRYPT_MODE, publicKey);
            return cipher;
        }

        public Cipher getWriterCipher() throws Exception {
            AsymmetricEncryptionConfig aec = ioService.getAsymmetricEncryptionConfig();
            Cipher cipher = Cipher.getInstance(algorithm);
            KeyStore.PrivateKeyEntry pkEntry = (KeyStore.PrivateKeyEntry)
                    keyStore.getEntry(aec.getKeyAlias(), new KeyStore.PasswordProtection(aec.getKeyPassword().toCharArray()));
            PrivateKey privateKey = pkEntry.getPrivateKey();
            cipher.init(Cipher.ENCRYPT_MODE, privateKey);
            return cipher;
        }

        public boolean isAsymmetric() {
            return true;
        }
    }

    static class SymmetricCipherBuilder implements CipherBuilder {
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
            char chars[] = saltStr.toCharArray();
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
                bbPass.put(md.digest(passPhrase.getBytes()));
                md.reset();
                byte[] saltDigest = md.digest(salt);
                bbPass.put(saltDigest);
                boolean isCBC = algorithm.indexOf("/CBC/") != -1;
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

        public Cipher getReaderCipher(String ignored) {
            return create(false);
        }

        public boolean isAsymmetric() {
            return false;
        }
    }

    public static void handleCipherException(Exception e, Connection connection) {
        logger.log(Level.WARNING, e.getMessage(), e);
        connection.close();
    }
}
