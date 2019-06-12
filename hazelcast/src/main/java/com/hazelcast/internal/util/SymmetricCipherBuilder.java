package com.hazelcast.internal.util;

import com.hazelcast.config.AbstractSymmetricEncryptionConfig;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.nio.Bits;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class SymmetricCipherBuilder {
    private static final int IV_LENGTH_CBC = 8;
    private static final int IV_LENGTH_AES = 16;

    private final String algorithm;
    private final String passPhrase;
    // 8-byte Salt
    private final byte[] salt;
    private final int iterationCount;

    public SymmetricCipherBuilder(AbstractSymmetricEncryptionConfig<?> config) {
        algorithm = config.getAlgorithm();
        passPhrase = config.getPassword();
        salt = createSalt(config.getSalt());
        iterationCount = config.getIterationCount();
    }

    public Cipher create(boolean encryptMode, byte[] keyBytesRaw) {
        try {
            int mode = (encryptMode) ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE;
            Cipher cipher = Cipher.getInstance(algorithm);
            String keyAlgorithm = findKeyAlgorithm(algorithm);
            AtomicReference<byte[]> keyBytesRef = new AtomicReference<>(keyBytesRaw);
            byte[] saltDigest = buildKeyBytes(keyBytesRef);
            byte[] keyBytes = keyBytesRef.get();

            SecretKey key = null;
            // CBC mode requires IvParameter with 8 byte input
            int ivLength = IV_LENGTH_CBC;
            AlgorithmParameterSpec paramSpec = null;

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
            } else if (algorithm.startsWith("PBEWith")) {
                paramSpec = new PBEParameterSpec(salt, iterationCount);
                KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), salt, iterationCount);
                key = SecretKeyFactory.getInstance(keyAlgorithm).generateSecret(keySpec);
            }
            paramSpec = buildFinalAlgorithmParameterSpec(saltDigest, ivLength, paramSpec);
            cipher.init(mode, key, paramSpec);
            return cipher;
        } catch (Throwable e) {
            throw new RuntimeException("Unable to create Cipher: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private byte[] buildKeyBytes(AtomicReference<byte[]> keyBytes)
            throws NoSuchAlgorithmException {
        // 32-bit digest key = pass + salt
        ByteBuffer bbPass = ByteBuffer.allocate(32);
        MessageDigest md = MessageDigest.getInstance("MD5");
        bbPass.put(md.digest(stringToBytes(passPhrase)));
        md.reset();
        byte[] saltDigest = md.digest(salt);
        bbPass.put(saltDigest);
        if (keyBytes.get() == null) {
            keyBytes.set(bbPass.array());
        }
        return saltDigest;
    }

    private AlgorithmParameterSpec buildFinalAlgorithmParameterSpec(byte[] saltDigest, int ivLength,
                                                                    AlgorithmParameterSpec paramSpec) {
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

    static String findKeyAlgorithm(String algorithm) {
        if (algorithm.indexOf('/') != -1) {
            return algorithm.substring(0, algorithm.indexOf('/'));
        }
        return algorithm;
    }

}
