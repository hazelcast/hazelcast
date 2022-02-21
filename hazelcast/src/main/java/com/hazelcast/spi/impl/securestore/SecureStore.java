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

package com.hazelcast.spi.impl.securestore;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A Secure Store abstraction for interacting with systems that manage symmetric
 * encryption keys.
 */
public interface SecureStore {

    /**
     * Returns a list of encryption keys, where the first item represents the current
     * active key and the other items represent previous keys (used for decryption
     * of old chunks).
     */
    @Nonnull
    List<byte[]> retrieveEncryptionKeys();

    /**
     * Registers an {@link EncryptionKeyListener} to be notified of encryption key
     * changes.
     *
     * @param listener an {@link EncryptionKeyListener} instance
     */
    void addEncryptionKeyListener(@Nonnull EncryptionKeyListener listener);

    /**
     * Secure Store listener to get notified of changes to encryption keys.
     *
     * @see SecureStore#addEncryptionKeyListener(EncryptionKeyListener)
     */
    interface EncryptionKeyListener {
        /**
         * Called when the encryption key in the Secure Store changes.
         *
         * @param key the new encryption key
         */
        void onEncryptionKeyChange(byte[] key);
    }

}
