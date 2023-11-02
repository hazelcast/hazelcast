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

package com.hazelcast.internal.util;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static com.hazelcast.internal.nio.IOUtil.getFileFromResourcesAsStream;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.JVMUtil.is32bitJVM;
import static com.hazelcast.internal.tpcengine.util.OS.osName;
import static com.hazelcast.internal.tpcengine.util.OS.isUnixFamily;

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
        try (InputStream src = getFileFromResourcesAsStream(getBundledLibraryPath())) {
            File dest = File.createTempFile("hazelcast-libicmp-helper-", ".so");

            Files.copy(src, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);

            return dest.getAbsolutePath();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static String getBundledLibraryPath() {
        if (!isUnixFamily()) {
            throw new IllegalStateException("ICMP not supported in this platform: " + osName());
        }

        return is32bitJVM()
                ? "lib/linux-x86/libicmp_helper.so"
                : "lib/linux-x86_64/libicmp_helper.so";
    }
}
