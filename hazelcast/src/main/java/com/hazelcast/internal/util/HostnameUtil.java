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

import com.hazelcast.function.SupplierEx;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HostnameUtil contains hostname helper methods
 */
public final class HostnameUtil {

    public static final int PROCESS_TIMEOUT_IN_SECONDS = 5;

    private HostnameUtil() {
    }

    /**
     * Resolves local hostname
     *
     * @return local hostname or null if it can't be resolved
     */
    public static String getLocalHostname() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname == null) {
            hostname = getOrNull(HostnameUtil::execHostnameCmd);
        }
        if (hostname == null) {
            hostname = getOrNull(() -> InetAddress.getLocalHost().getHostName());
        }
        return shortHostname(hostname);
    }

    @Nonnull
    private static String execHostnameCmd() throws Exception {
        Process exec = Runtime.getRuntime().exec("hostname");
        exec.waitFor(PROCESS_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        InputStream stream = exec.getInputStream();
        return new BufferedReader(new InputStreamReader(stream)).lines().collect(Collectors.joining("\n"));
    }

    private static String shortHostname(String hostname) {
        if (hostname == null) {
            return null;
        }
        if (hostname.contains(".")) {
            hostname = hostname.substring(0, hostname.indexOf("."));
        }
        return hostname;
    }

    private static String getOrNull(SupplierEx<String> supplierEx) {
        try {
            return supplierEx.getEx();
        } catch (Exception e) {
            return null;
        }
    }
}
