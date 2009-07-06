/*
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.nio;

import com.hazelcast.config.AsymmetricEncryptionConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.SymmetricEncryptionConfig;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.security.*;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

final class CipherHelper {
    static AsymmetricCipherBuilder asymmetricCipherBuilder = null;
    static SymmetricCipherBuilder symmetricCipherBuilder = null;

    public static synchronized Cipher createAsymmetricReaderCipher(String remoteAlias) throws Exception {
        if (asymmetricCipherBuilder == null) {
            asymmetricCipherBuilder = new AsymmetricCipherBuilder();
        }
        return asymmetricCipherBuilder.getReaderCipher(remoteAlias);
    }

    public static synchronized Cipher createAsymmetricWriterCipher() throws Exception {
        if (asymmetricCipherBuilder == null) {
            asymmetricCipherBuilder = new AsymmetricCipherBuilder();
        }
        return asymmetricCipherBuilder.getWriterCipher();
    }

    public static synchronized Cipher createSymmetricReaderCipher() throws Exception {
        if (symmetricCipherBuilder == null) {
            symmetricCipherBuilder = new SymmetricCipherBuilder();
        }
        return symmetricCipherBuilder.getReaderCipher(null);
    }

    public static synchronized Cipher createSymmetricWriterCipher() throws Exception {
        if (symmetricCipherBuilder == null) {
            symmetricCipherBuilder = new SymmetricCipherBuilder();
        }
        return symmetricCipherBuilder.getWriterCipher();
    }

    public static boolean isAsymmetricEncryptionEnabled() {
        AsymmetricEncryptionConfig aec = Config.get().getNetworkConfig().getAsymmetricEncryptionConfig();
        return (aec != null && aec.isEnabled());
    }

    public static boolean isSymmetricEncryptionEnabled() {
        SymmetricEncryptionConfig sec = Config.get().getNetworkConfig().getSymmetricEncryptionConfig();
        return (sec != null && sec.isEnabled());
    }

    public static String getKeyAlias() {
        AsymmetricEncryptionConfig aec = Config.get().getNetworkConfig().getAsymmetricEncryptionConfig();
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

        AsymmetricCipherBuilder() {
            try {
                String provider = "org.bouncycastle.jce.provider.BouncyCastleProvider";
                Security.addProvider((Provider) Class.forName(provider).newInstance());
                AsymmetricEncryptionConfig aec = Config.get().getNetworkConfig().getAsymmetricEncryptionConfig();

                algorithm = aec.getAlgorithm();
                keyStore = KeyStore.getInstance(aec.getStoreType());

                // get user password and file input stream
                char[] password = aec.getStorePassword().toCharArray();
                java.io.FileInputStream fis =
                        new java.io.FileInputStream(aec.getStorePath());
                keyStore.load(fis, password);

                fis.close();
            } catch (Exception e) {
                e.printStackTrace();
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
            AsymmetricEncryptionConfig aec = Config.get().getNetworkConfig().getAsymmetricEncryptionConfig();
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

        SymmetricCipherBuilder() {
            SymmetricEncryptionConfig sec = Config.get().getNetworkConfig().getSymmetricEncryptionConfig();
            algorithm = sec.getAlgorithm();
            passPhrase = sec.getPassword();
            salt = createSalt(sec.getSalt());
            iterationCount = sec.getIterationCount(); 
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
                KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), salt, iterationCount);
                SecretKey key = SecretKeyFactory.getInstance(
                        algorithm).generateSecret(keySpec);
                Cipher cipher = Cipher.getInstance(key.getAlgorithm());
                AlgorithmParameterSpec paramSpec = new PBEParameterSpec(salt, iterationCount);
                cipher.init((encryptMode) ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, key, paramSpec);

                return cipher;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
            return null;
        }

        public Cipher getWriterCipher() {
            return create(true);
        }

        public Cipher getReaderCipher(String ignored) {
            return create (false);
        }

        public boolean isAsymmetric() {
            return false;
        }

    }

    public static void handleCipherException(Exception e, Connection connection) {
        e.printStackTrace();
        connection.close();
    }
}
