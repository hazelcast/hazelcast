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

package com.hazelcast.jet.function;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A function that accepts a JDBC connection to the database, the total
 * parallelism and processor index as arguments and produces a result set.
 * This result set should return a part of the whole result set specific to
 * this processor.
 *
 * @since Jet 3.0
 */
@FunctionalInterface
public interface ToResultSetFunction extends Serializable {

    /**
     * Creates a result set which returns a part of the rows pertaining to the
     * given processor.
     *
     * @param connection the JDBC connection
     * @param parallelism the total parallelism for the processor
     * @param index the global processor index
     */
    ResultSet createResultSet(Connection connection, int parallelism, int index) throws SQLException;
}
