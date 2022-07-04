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

package com.hazelcast.config.replacer;

import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * The common parent for {@link ConfigReplacer} implementations which allow to mask values by encrypting the value. This parent
 * class contains shared methods responsible for encryption/decryption. The implementing classes have to provide
 * {@link #getPassword()} implementation - the password will be used to generate a secret key.
 */
public abstract class AbstractPbeReplacer implements ConfigReplacer {

    /**
     * Replacer property name to configure {@link Cipher} algorithm name.
     */
    public static final String PROPERTY_CIPHER_ALGORITHM = "cipherAlgorithm";
    /**
     * Replacer property name to configure {@link SecretKeyFactory} algorithm name.
     */
    public static final String PROPERTY_SECRET_KEY_FACTORY_ALGORITHM = "secretKeyFactoryAlgorithm";
    /**
     * Replacer property name to configure {@link SecretKeySpec} algorithm name.
     */
    public static final String PROPERTY_SECRET_KEY_ALGORITHM = "secretKeyAlgorithm";
    /**
     * Replacer property name to configure key length (in bits).
     */
    public static final String PROPERTY_KEY_LENGTH_BITS = "keyLengthBits";
    /**
     * Replacer property name to configure salt length (in bytes).
     */
    public static final String PROPERTY_SALT_LENGTH_BYTES = "saltLengthBytes";
    /**
     * Replacer property name to configure Java Security provider name used for {@link Cipher} and {@link SecretKeyFactory}
     * selection.
     */
    public static final String PROPERTY_SECURITY_PROVIDER = "securityProvider";

    /**
     * Default value for {@value #PROPERTY_CIPHER_ALGORITHM} property.
     */
    public static final String DEFAULT_CIPHER_ALGORITHM = "AES";
    /**
     * Default value for {@value #PROPERTY_SECRET_KEY_FACTORY_ALGORITHM} property.
     */
    public static final String DEFAULT_SECRET_KEY_FACTORY_ALGORITHM = "PBKDF2WithHmacSHA256";

    private final ILogger logger = Logger.getLogger(AbstractPbeReplacer.class);

    private String cipherAlgorithm;
    private String secretKeyFactoryAlgorithm;
    private String secretKeyAlgorithm;
    private String securityProvider;
    private int keyLengthBits;
    private int saltLengthBytes;

    @Override
    public void init(Properties properties) {
        securityProvider = properties.getProperty(PROPERTY_SECURITY_PROVIDER);
        cipherAlgorithm = properties.getProperty(PROPERTY_CIPHER_ALGORITHM, DEFAULT_CIPHER_ALGORITHM);
        secretKeyFactoryAlgorithm = properties.getProperty(PROPERTY_SECRET_KEY_FACTORY_ALGORITHM,
                DEFAULT_SECRET_KEY_FACTORY_ALGORITHM);
        secretKeyAlgorithm = properties.getProperty(PROPERTY_SECRET_KEY_ALGORITHM, DEFAULT_CIPHER_ALGORITHM);
        keyLengthBits = Integer.parseInt(properties.getProperty(PROPERTY_KEY_LENGTH_BITS, "128"));
        saltLengthBytes = Integer.parseInt(properties.getProperty(PROPERTY_SALT_LENGTH_BYTES, "8"));
        checkPositive(keyLengthBits, "Key length has to be positive number");
        checkPositive(saltLengthBytes, "Salt length has to be positive number");
    }

    /**
     * Provides password for a chosen SecretKeyFactory.
     *
     * @return password must not be {@code null} or empty
     */
    protected abstract char[] getPassword();

    @Override
    public String getReplacement(String variable) {
        try {
            return decrypt(variable);
        } catch (Exception e) {
            logger.warning("Unable to decrypt variable " + variable, e);
        }
        return null;
    }

    /**
     * Encrypts given string with key generated from {@link #getPassword()} with given iteration count and return the masked
     * value (to be used as the variable).
     *
     * @param secretStr sensitive string to be protected by encryption
     * @param iterations iteration count
     * @return the encrypted value.
     * @throws Exception in case of any exceptional case
     */
    protected String encrypt(String secretStr, int iterations) throws Exception {
        SecureRandom secureRandom = new SecureRandom();
        byte[] salt = new byte[saltLengthBytes];
        secureRandom.nextBytes(salt);
        byte[] encryptedVal = transform(Cipher.ENCRYPT_MODE, secretStr.getBytes(StandardCharsets.UTF_8), salt, iterations);
        return new String(Base64.getEncoder().encode(salt), StandardCharsets.UTF_8) + ":" + iterations + ":"
                + new String(Base64.getEncoder().encode(encryptedVal), StandardCharsets.UTF_8);
    }

    /**
     * Decrypts given encrypted variable.
     *
     * @param encryptedStr the encrypted value
     * @return the decrypted value
     * @throws Exception in case of any exceptional case
     */
    protected String decrypt(String encryptedStr) throws Exception {
        String[] split = encryptedStr.split(":");
        checkTrue(split.length == 3, "Wrong format of the encrypted variable (" + encryptedStr + ")");
        byte[] salt = Base64.getDecoder().decode(split[0].getBytes(StandardCharsets.UTF_8));
        checkTrue(salt.length == saltLengthBytes, "Salt length doesn't match.");
        int iterations = Integer.parseInt(split[1]);
        byte[] encryptedVal = Base64.getDecoder().decode(split[2].getBytes(StandardCharsets.UTF_8));
        return new String(transform(Cipher.DECRYPT_MODE, encryptedVal, salt, iterations), StandardCharsets.UTF_8);
    }

    /**
     * Encrypt/decrypt given value by using the configured cipher algorithm with provided salt and iterations count.
     *
     * @param cryptMode mode (one of {@link Cipher#DECRYPT_MODE}, {@link Cipher#ENCRYPT_MODE})
     * @param value value to encrypt/decrypt
     * @param salt salt to be used
     * @param iterations count of iterations
     * @return encrypted or decrypted byte array (depending on cryptMode)
     */
    private byte[] transform(int cryptMode, byte[] value, byte[] salt, int iterations) throws Exception {
        checkPositive(iterations, "Count of iterations has to be positive number.");
        SecretKeyFactory factory = SecretKeyFactory.getInstance(secretKeyFactoryAlgorithm);
        char[] password = getPassword();
        checkTrue(password != null && password.length > 0, "Empty password is not supported");
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password, salt, iterations, keyLengthBits);
        byte[] tmpKey = factory.generateSecret(pbeKeySpec).getEncoded();
        SecretKeySpec secretKeySpec = new SecretKeySpec(tmpKey, secretKeyAlgorithm);
        Cipher cipher = securityProvider == null ? Cipher.getInstance(cipherAlgorithm)
                : Cipher.getInstance(cipherAlgorithm, securityProvider);
        cipher.init(cryptMode, secretKeySpec);
        return cipher.doFinal(value);
    }
}
