/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import java.util.Collection;

/**
 * Utility class for closing Closable objects.
 */
public final class CloseUtil {

    private CloseUtil() {
    }

    /**
     * Closes the collection of AutoClosables quietly.
     * <p/>
     * If the collection is null, the call is ignored.
     * <p/>
     * If an element in the collection is null, that element is ignored.
     * <p/>
     * If the Closable throws an exception, the exception is ignored and closing moves to the next element.
     *
     * @param collection the collections of AutoClosable to close. When null, the call is ignored. Null items
     *                   in the collection are also ignored.
     */
    public static void closeAllQuietly(Collection<? extends AutoCloseable> collection) {
        if (collection == null) {
            return;
        }

        for (AutoCloseable closeable : collection) {
            closeQuietly(closeable);
        }
    }

    /**
     * Closes an AutoClosable quietly.
     * <p/>
     * If the closable is null, the call is ignored.
     * <p/>
     * If {@link AutoCloseable#close()} throws an exception, it is ignored.
     *
     * @param closeable the AutoClosable to close.
     */
    @SuppressWarnings("java:S108")
    public static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (Exception ignore) {
        }
    }
}
