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

package com.hazelcast.security.permission;

import com.hazelcast.jet.impl.util.IOUtil;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;

import javax.annotation.Nullable;

public class ConnectorPermission extends InstancePermission {

    public static final String FILE_PREFIX = "file:";
    public static final String SOCKET_PREFIX = "socket:";
    public static final String MONGO_PREFIX = "mongo:";

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

    /**
     * @param connectionDescription connection string or data connection name
     * @param databaseName database name this permission is about, null if permission is granted to all databases
     * @param collectionName collection name this permission is about, null if permission is granted to all collections
     * @param action action this permission is about
     */
    public static ConnectorPermission mongo(@Nullable String connectionDescription,
                                            @Nullable String databaseName, @Nullable String collectionName,
                                            String action) {
        String db = databaseName == null ? "$ANY$" : databaseName;
        String col = collectionName == null ? "$ANY$" : collectionName;
        String dbCol = db + "/" + col;
        String connectionDescNonNull = connectionDescription == null ? "$ANY$" : connectionDescription;
        return new ConnectorPermission(MONGO_PREFIX + connectionDescNonNull + ":" + dbCol, action);
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
