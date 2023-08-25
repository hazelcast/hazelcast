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

package com.hazelcast.sql.impl.client;

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.sql.SqlRowMetadata;

class SqlResubmissionResult {
    private final ClientConnection connection;
    private final SqlError sqlError;
    private final SqlRowMetadata rowMetadata;
    private final SqlPage rowPage;
    private final long updateCount;

    SqlResubmissionResult(SqlError sqlError) {
        this.sqlError = sqlError;
        this.connection = null;
        this.rowMetadata = null;
        this.rowPage = null;
        this.updateCount = 0;
    }

    SqlResubmissionResult(ClientConnection connection, SqlRowMetadata rowMetadata, SqlPage rowPage, long updateCount) {
        this.connection = connection;
        this.rowMetadata = rowMetadata;
        this.rowPage = rowPage;
        this.updateCount = updateCount;
        this.sqlError = null;
    }

    ClientConnection getConnection() {
        return connection;
    }

    SqlError getSqlError() {
        return sqlError;
    }

    SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }

    SqlPage getRowPage() {
        return rowPage;
    }

    long getUpdateCount() {
        return updateCount;
    }
}
