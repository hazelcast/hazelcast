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
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static com.hazelcast.internal.util.BasicSymmetricCipherBuilder.findKeyAlgorithm;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicSymmetricCipherBuilderTest {

    @Parameter
    public String algorithm;

    @Parameter(1)
    public int keySize;

    @Parameters(name = "algorithm:{0}, keySize:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {"AES", 128},
                {"AES/CBC/NoPadding", 128},
                {"AES/CBC/PKCS5Padding", 128},
                {"AES/ECB/NoPadding", 128},
                {"AES/ECB/PKCS5Padding", 128},
                {"Blowfish", 128},
                {"DESede", 112},
                {"DESede/CBC/PKCS5Padding", 112},
                {"DESede/CBC/NoPadding", 112},
                {"DESede", 168},
                {"DESede/CBC/PKCS5Padding", 168},
                {"DESede/CBC/NoPadding", 168},
                {"DES", 56},
        });
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testCreateCipher() throws Exception {
        AbstractBasicSymmetricEncryptionConfig config = config();
        Cipher cipher = createCipher(config, true, getKey());
        assertEquals(algorithm, cipher.getAlgorithm());
    }

    @Test
    public void testEncryptDecrypt() throws Exception {
        AbstractBasicSymmetricEncryptionConfig config = config();
        byte[] data = new byte[16 * 1024];
        new Random().nextBytes(data);
        byte[] key = getKey();
        Cipher encryptCipher = createCipher(config, true, key);
        Cipher decryptCipher = createCipher(config, false, key);
        assertArrayEquals(data, decryptCipher.doFinal(encryptCipher.doFinal(data)));
    }

    @Test
    public void testEncryptDecrypt_whenNullKey() throws Exception {
        // null key triggers key auto-generation
        AbstractBasicSymmetricEncryptionConfig config = config();
        expected.expect(NullPointerException.class);
        expected.expectMessage("Key bytes cannot be null");
        createCipher(config, true, null);
    }

    protected Cipher createCipher(AbstractBasicSymmetricEncryptionConfig config, boolean encryptMode, byte[] key) {
        return builder(config).create(encryptMode, key);
    }

    protected AbstractBasicSymmetricEncryptionConfig<?> config() {
        return new AbstractBasicSymmetricEncryptionConfig<AbstractBasicSymmetricEncryptionConfig>() { }
                .setEnabled(true)
                .setAlgorithm(algorithm);
    }

    protected BasicSymmetricCipherBuilder builder(AbstractBasicSymmetricEncryptionConfig<?> config) {
        return new BasicSymmetricCipherBuilder(config);
    }

    private byte[] getKey() throws Exception {
        if (keySize == 0) {
            return null;
        }
        KeyGenerator keyGenerator = KeyGenerator.getInstance(findKeyAlgorithm(algorithm));
        keyGenerator.init(keySize);
        SecretKey secretKey = keyGenerator.generateKey();
        return secretKey.getEncoded();
    }
}
