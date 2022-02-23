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

package com.hazelcast.security.permission;

import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;

import javax.annotation.Nullable;

public class ConnectorPermission extends InstancePermission {

    public static final String FILE_PREFIX = "file:";
    public static final String SOCKET_PREFIX = "socket:";
    public static final String JMS_PREFIX = "jms:";
    public static final String JDBC_PREFIX = "jdbc:";

    private static final int READ = 1;
    private static final int WRITE = 2;
    private static final int ALL = READ | WRITE;

    public ConnectorPermission(String name, String... actions) {
        super(name, actions);
    }

    /**
     * converts the {@code directory} to canonical path if it is not
     * one of the Hadoop prefixes.
     * see {@link FileSourceBuilder#hasHadoopPrefix(String)}
     */
    public static ConnectorPermission file(String directory, String action) {
        String canonicalPath = directory;
        if (!FileSourceBuilder.hasHadoopPrefix(directory)) {
            canonicalPath = IOUtil.canonicalName(directory);
        }
        return new ConnectorPermission(FILE_PREFIX + canonicalPath, action);
    }

    public static ConnectorPermission socket(String host, int port, String action) {
        return new ConnectorPermission(SOCKET_PREFIX + host + ':' + port, action);
    }

    public static ConnectorPermission jms(@Nullable String destination, String action) {
        return new ConnectorPermission(JMS_PREFIX + (destination == null ? "" : destination), action);
    }

    public static ConnectorPermission jdbc(@Nullable String connectionUrl, String action) {
        return new ConnectorPermission(JDBC_PREFIX + (connectionUrl == null ? "" : connectionUrl), action);
    }

    @Override
    protected int initMask(String[] actions) {
        int mask = NONE;
        for (String action : actions) {
            if (ActionConstants.ACTION_ALL.equals(action)) {
                return ALL;
            }
            if (ActionConstants.ACTION_READ.equals(action)) {
                mask |= READ;
            } else if (ActionConstants.ACTION_WRITE.equals(action)) {
                mask |= WRITE;
            } else {
                throw new IllegalArgumentException("Configured action[" + action + "] is not supported");
            }
        }
        return mask;
    }
}
