/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.impl.Build;
import static com.hazelcast.impl.Constants.IO.BYTE_BUFFER_SIZE;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class AbstractSelectionHandler implements SelectionHandler {

    protected static Logger logger = Logger.getLogger(AbstractSelectionHandler.class.getName());

    public static final int RECEIVE_SOCKET_BUFFER_SIZE = 32 * BYTE_BUFFER_SIZE;

    public static final int SEND_SOCKET_BUFFER_SIZE = 32 * BYTE_BUFFER_SIZE;

    public static final boolean DEBUG = Build.DEBUG;

    protected SocketChannel socketChannel;

    protected InSelector inSelector;

    protected OutSelector outSelector;

    protected Connection connection;

    protected SelectionKey sk = null;

    protected ByteBuffer cipherBuffer = null;

    protected Cipher cipher = null;

    protected boolean cipherEnabled = false;

    public AbstractSelectionHandler(final Connection connection, boolean writer) {
        super();
        this.connection = connection;
        this.socketChannel = connection.getSocketChannel();
        this.inSelector = InSelector.get();
        this.outSelector = OutSelector.get();
        if (cipherEnabled) {
            this.cipher = createCipher("hazelcast", writer);
            if (writer) {
                cipherBuffer = ByteBuffer.allocate(2 * SEND_SOCKET_BUFFER_SIZE);
            } else {
                cipherBuffer = ByteBuffer.allocate(2 * RECEIVE_SOCKET_BUFFER_SIZE);
            }
        }
    }

    protected void shutdown() {

    }

    final void handleSocketException(final Exception e) {
        if (DEBUG) {
            logger.log(Level.FINEST,
                    Thread.currentThread().getName() + " Closing Socket. cause:  ", e);
        }

        if (sk != null)
            sk.cancel();
        connection.close();
    }

    final void registerOp(final Selector selector, final int operation) {
        try {
            if (!connection.live())
                return;
            if (sk == null) {
                sk = socketChannel.register(selector, operation, this);
            } else {
                sk.interestOps(operation);
            }
        } catch (final Exception e) {
            handleSocketException(e);
        }
    }


    // 8-byte Salt
    byte[] salt = {
            (byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32,
            (byte) 0x56, (byte) 0x35, (byte) 0xE3, (byte) 0x03
    };

    public Cipher createCipher(String passPhrase, boolean encrypt) {
        try {
            int iterationCount = 32;
            // Create the key
            KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), salt, iterationCount);
            SecretKey key = SecretKeyFactory.getInstance(
                    "PBEWithMD5AndDES").generateSecret(keySpec);
            Cipher cipher = Cipher.getInstance(key.getAlgorithm());

            // Prepare the parameter to the ciphers
            AlgorithmParameterSpec paramSpec = new PBEParameterSpec(salt, iterationCount);

            // Create the ciphers
            cipher.init((encrypt) ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, key, paramSpec);

//            Security.addProvider(new BouncyCastleProvider());
//
//
//            String xform = "RSA/NONE/PKCS1Padding";
//            // Generate a key-pair
////            KeyPairGenerator kpg = KeyPairGenerator.getInstance("DESede");
////            kpg.initialize(112); // 512 is the keysize.
////            KeyPair kp = kpg.generateKeyPair();
////            PublicKey pubk = kp.getPublic();
////            PrivateKey prvk = kp.getPrivate();
//
//            SecretKey key = KeyGenerator.getInstance("DESede").generateKey();
//            Cipher cipher = Cipher.getInstance(xform);
//
//            if (encrypt) {
//                cipher.init (Cipher.ENCRYPT_MODE, key);
//            } else {
//                cipher.init (Cipher.DECRYPT_MODE, key);
//            }

            return cipher;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return null;
    }

}
