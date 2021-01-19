/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.cdc;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;

/**
 * Demonstrates a simple cache which uses change data capture to monitor a
 * MySQL database and maintain an up-to-date cache of its content in memory.
 * <p>
 * To have a database to experiment with start one using following command:
 * <pre>
 *  docker run -it --rm --name mysql -p 3306:3306 \
 *     -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser \
 *     -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.3
 * </pre>
 * <p>
 * To have a command line client to generate some database events manually use:
 * <pre>
 *     docker run -it --rm --name mysqlterm --link mysql --rm mysql:5.7 sh \
 *     -c 'exec mysql -h"$MYSQL_PORT_3306_TCP_ADDR" -P"$MYSQL_PORT_3306_TCP_PORT" \
 *     -uroot -p"$MYSQL_ENV_MYSQL_ROOT_PASSWORD"'
 * </pre>
 * <p>
 * The map written into by this pipeline's sink can be read from other Jet jobs
 * or IMDG clients as any other {@code IMap}.
 */
public class Cache {

    public static void main(String[] args) {
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("source")
                .setDatabaseAddress("127.0.0.1")
                .setDatabasePort(3306)
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1")
                .setDatabaseWhitelist("inventory")
                .setTableWhitelist("inventory.customers")
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .peek()
                .writeTo(CdcSinks.map("customers",
                        r -> r.key().toMap().get("id"),
                        r -> r.value().toObject(Customer.class).toString())
                );

        JobConfig cfg = new JobConfig().setName("mysql-monitor");
        JetInstance jet = Jet.bootstrappedInstance();
        jet.newJobIfAbsent(pipeline, cfg).join();
    }

}
