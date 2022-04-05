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

package com.hazelcast.internal.util;

import java.io.File;
import java.io.InputStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.copy;
import static com.hazelcast.internal.nio.IOUtil.getFileFromResourcesAsStream;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.JVMUtil.is32bitJVM;
import static com.hazelcast.internal.util.OsHelper.OS;
import static com.hazelcast.internal.util.OsHelper.isUnixFamily;

/**
 * Helper class that uses JNI to check whether the JVM process has enough permission to create raw-sockets.
 */
public final class ICMPHelper {

    static {
        System.load(extractBundledLib());
    }

    private ICMPHelper() {
    }

    private static native boolean isRawSocketPermitted0();

    public static boolean isRawSocketPermitted() {
        return isRawSocketPermitted0();
    }

    private static String extractBundledLib() {
        InputStream src = null;
        try {
            src = getFileFromResourcesAsStream(getBundledLibraryPath());
            File dest = File.createTempFile("hazelcast-libicmp-helper-", ".so");

            copy(src, dest);

            return dest.getAbsolutePath();
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            closeResource(src);
        }
    }

    private static String getBundledLibraryPath() {
        if (!isUnixFamily()) {
            throw new IllegalStateException("ICMP not supported in this platform: " + OS);
        }

        return is32bitJVM()
                ? "lib/linux-x86/libicmp_helper.so"
                : "lib/linux-x86_64/libicmp_helper.so";
    }
}
