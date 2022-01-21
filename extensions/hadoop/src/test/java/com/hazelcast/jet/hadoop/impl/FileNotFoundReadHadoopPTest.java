/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.hadoop.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Lists.newArrayList;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class FileNotFoundReadHadoopPTest extends HadoopTestSupport {

    @Parameterized.Parameter
    public Class inputFormatClass;

    private java.nio.file.Path directory;
    private Set<org.apache.hadoop.fs.Path> paths = new HashSet<>();

    @Parameterized.Parameters(name = "inputFormat={0}")
    public static Collection<Object[]> parameters() {
        return newArrayList(new Object[][]{
                {TextInputFormat.class},
                {org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class}
        });
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void setup() throws IOException {
        directory = Files.createTempDirectory(getClass().getSimpleName());
    }

    @After
    public void after() {
        if (directory != null) {
            IOUtil.delete(directory.toFile());
            directory = null;
        }
    }

    private Configuration configuration(boolean ignoreFileNotFound) throws IOException {
        if (this.inputFormatClass.getPackage().getName().contains("mapreduce")) {
            Job job = Job.getInstance();
            job.setInputFormatClass(inputFormatClass);
            for (org.apache.hadoop.fs.Path path : this.paths) {
                org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, path);
            }
            job.getConfiguration().setBoolean(HadoopSources.IGNORE_FILE_NOT_FOUND, ignoreFileNotFound);
            return job.getConfiguration();
        } else {
            JobConf jobConf = new JobConf();
            jobConf.setInputFormat(inputFormatClass);
            for (org.apache.hadoop.fs.Path path : this.paths) {
                org.apache.hadoop.mapred.FileInputFormat.addInputPath(jobConf, path);
            }
            jobConf.setBoolean(HadoopSources.IGNORE_FILE_NOT_FOUND, ignoreFileNotFound);
            return jobConf;
        }
    }

    @Test
    public void shouldNotFailForNonExistingPath() throws IOException {
        paths.add(new org.apache.hadoop.fs.Path(directory + File.separator + "not-exists"));
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.inputFormat(configuration(true), (k, v) -> v.toString()))
         .setLocalParallelism(1)
         .writeTo(Sinks.logger());

        // Should just pass, no need to check results as there are none
        instance().getJet().newJob(p).join();
    }

    @Test
    public void shouldFailForNonExistingPathWithFlag() throws IOException {
        paths.add(new org.apache.hadoop.fs.Path(directory + File.separator + "not-exists"));
        Pipeline p = Pipeline.create();
        p.readFrom(HadoopSources.inputFormat(configuration(false), (k, v) -> v.toString()))
         .setLocalParallelism(1)
         .writeTo(Sinks.logger());

        assertThatThrownBy(() -> instance().getJet().newJob(p).join())
                .hasRootCauseInstanceOf(JetException.class)
                .hasMessageContaining("matches no files");
    }
}
