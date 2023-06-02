/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static java.util.Arrays.asList;

public class SqlStatefulDagTest extends SqlTestSupport {
    private static MiniDFSCluster cluster;

    @BeforeClass
    public static void setup() throws IOException {
        assumeThatNoWindowsOS();
        assumeHadoopSupportsIbmPlatform();

        initialize(1, null);

        File directory = Files.createTempDirectory("sql-test-hdfs").toFile().getAbsoluteFile();
        directory.deleteOnExit();

        Configuration configuration = new Configuration();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, directory.getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(configuration).build();
        cluster.waitClusterUp();
    }

    @Test
    public void testReadHadoop() throws IOException {
        store("/csv/file.csv", "id,name\n1,Alice\n2,Bob");

        String name = randomName();
        instance().getSql().execute(
                "CREATE MAPPING " + name + " (id INT, name VARCHAR) " +
                "TYPE File " +
                "OPTIONS (" +
                "  'format' = 'csv'," +
                "  'path' = '" + cluster.getFileSystem().getUri() + "/csv'" +
                ");");

        for (int i = 0; i < 2; i++) {
            assertRowsAnyOrder("SELECT * FROM " + name, asList(
                    new Row(1, "Alice"),
                    new Row(2, "Bob")));
        }
    }

    @AfterClass
    public static void cleanup() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    private static void store(String path, String content) throws IOException {
        try (FSDataOutputStream output = cluster.getFileSystem().create(new Path(path))) {
            output.writeBytes(content);
        }
    }
}
