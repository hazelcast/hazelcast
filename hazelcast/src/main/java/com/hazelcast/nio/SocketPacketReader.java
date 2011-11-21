/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.copyToHeapBuffer;

class SocketPacketReader implements SocketReader {

    Packet packet = null;

    final PacketReader packetReader;
    final Connection connection;
    final IOService ioService;
    final SocketChannel socketChannel;
    final ILogger logger;

    public SocketPacketReader(SocketChannel socketChannel, Connection connection) {
        this.connection = connection;
        this.ioService = connection.connectionManager.ioService;
        this.socketChannel = socketChannel;
        this.logger = ioService.getLogger(SocketPacketReader.class.getName());
        boolean symmetricEncryptionEnabled = CipherHelper.isSymmetricEncryptionEnabled(ioService);
        boolean asymmetricEncryptionEnabled = CipherHelper.isAsymmetricEncryptionEnabled(ioService);
        if (asymmetricEncryptionEnabled || symmetricEncryptionEnabled) {
            if (asymmetricEncryptionEnabled && symmetricEncryptionEnabled) {
                packetReader = new ComplexCipherPacketReader();
                logger.log(Level.INFO, "Reader started with ComplexEncryption");
            } else if (symmetricEncryptionEnabled) {
                packetReader = new SymmetricCipherPacketReader();
                logger.log(Level.INFO, "Reader started with SymmetricEncryption");
            } else {
                packetReader = new AsymmetricCipherPacketReader();
                logger.log(Level.INFO, "Reader started with AsymmetricEncryption");
            }
        } else {
            packetReader = new DefaultPacketReader();
        }
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        packetReader.readPacket(inBuffer);
    }

    public void enqueueFullPacket(final Packet p) {
        p.flipBuffers();
        p.read();
        p.setFromConnection(connection);
        System.out.println("connection is client " + p.client);
        if (p.client) {
            connection.setType(Connection.Type.JAVA_CLIENT);
            ioService.handleClientPacket(p);
        } else {
            connection.setType(Connection.Type.MEMBER);
            ioService.handleMemberPacket(p);
        }
    }

    interface PacketReader {
        void readPacket(ByteBuffer inBuffer) throws Exception;
    }

    class DefaultPacketReader implements PacketReader {
        public void readPacket(ByteBuffer inBuffer) {
            while (inBuffer.hasRemaining()) {
                if (packet == null) {
                    packet = obtainReadable();
                }
                boolean complete = packet.read(inBuffer);
                if (complete) {
                    enqueueFullPacket(packet);
                    packet = null;
                }
            }
        }
    }

    class ComplexCipherPacketReader implements PacketReader {

        ComplexCipherPacketReader() {
        }

        public void readPacket(ByteBuffer inBuffer) throws Exception {
        }
    }

    class AsymmetricCipherPacketReader implements PacketReader {
        Cipher cipher = null;
        ByteBuffer cipherBuffer = ByteBuffer.allocate(128);
        ByteBuffer bbAlias = null;
        boolean aliasSizeSet = false;

        public void readPacket(ByteBuffer inBuffer) throws Exception {
            if (cipher == null) {
                if (!aliasSizeSet) {
                    if (inBuffer.remaining() < 4) {
                        return;
                    } else {
                        int aliasSize = inBuffer.getInt();
                        bbAlias = ByteBuffer.allocate(aliasSize);
                    }
                }
                copyToHeapBuffer(inBuffer, bbAlias);
                if (!bbAlias.hasRemaining()) {
                    bbAlias.flip();
                    String remoteAlias = new String(bbAlias.array(), 0, bbAlias.limit());
                    cipher = CipherHelper.createAsymmetricReaderCipher(connection.connectionManager.ioService, remoteAlias);
                }
            }
            while (inBuffer.remaining() >= 128) {
                if (cipherBuffer.position() > 0) throw new RuntimeException();
                int oldLimit = inBuffer.limit();
                inBuffer.limit(inBuffer.position() + 128);
                int cipherReadSize = cipher.doFinal(inBuffer, cipherBuffer);
                inBuffer.limit(oldLimit);
                cipherBuffer.flip();
                while (cipherBuffer.hasRemaining()) {
                    if (packet == null) {
                        packet = obtainReadable();
                    }
                    boolean complete = packet.read(cipherBuffer);
                    if (complete) {
                        enqueueFullPacket(packet);
                        packet = null;
                    }
                }
                cipherBuffer.clear();
            }
        }
    }

    class SymmetricCipherPacketReader implements PacketReader {
        int size = -1;
        final Cipher cipher;
        ByteBuffer cipherBuffer = ByteBuffer.allocate(2 * RECEIVE_SOCKET_BUFFER_SIZE);

        SymmetricCipherPacketReader() {
            cipher = init();
        }

        Cipher init() {
            Cipher c = null;
            try {
                c = CipherHelper.createSymmetricReaderCipher(connection.connectionManager.ioService);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Symmetric Cipher for ReadHandler cannot be initialized.", e);
            }
            return c;
        }

        public void readPacket(ByteBuffer inBuffer) throws Exception {
            while (inBuffer.hasRemaining()) {
                try {
                    if (size == -1) {
                        if (inBuffer.remaining() < 4) return;
                        size = inBuffer.getInt();
                    }
                    int remaining = inBuffer.remaining();
                    if (remaining < size) {
                        cipher.update(inBuffer, cipherBuffer);
                        size -= remaining;
                    } else if (remaining == size) {
                        cipher.doFinal(inBuffer, cipherBuffer);
                        size = -1;
                    } else {
                        int oldLimit = inBuffer.limit();
                        int newLimit = inBuffer.position() + size;
                        inBuffer.limit(newLimit);
                        cipher.doFinal(inBuffer, cipherBuffer);
                        inBuffer.limit(oldLimit);
                        size = -1;
                    }
                } catch (ShortBufferException e) {
                    logger.log(Level.WARNING, e.getMessage(), e);
                }
                cipherBuffer.flip();
                while (cipherBuffer.hasRemaining()) {
                    if (packet == null) {
                        packet = obtainReadable();
                    }
                    boolean complete = packet.read(cipherBuffer);
                    if (complete) {
                        enqueueFullPacket(packet);
                        packet = null;
                    }
                }
                cipherBuffer.clear();
            }
        }
    }

    public Packet obtainReadable() {
        return connection.obtainPacket();
    }
}