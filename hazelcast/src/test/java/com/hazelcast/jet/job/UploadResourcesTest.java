/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.jet.job;

import classloading.domain.DummyPredicate;
import com.hazelcast.config.Config;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UploadResourcesTest extends JetTestSupport {

    @Test
    public void whenUploadResourcesDisabled_andClassNotFoundOnServer_thenJobFails() {
        //when
        Config config = smallInstanceConfig();
        config.getJetConfig().setUploadResources(false);
        JetInstance jet = createJetMember(config);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(DummyPredicate.class);
        jobConfig.setClassLoaderFactory(UploadResourcesTest::filteringClassloader);

        //then
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .filter(new DummyPredicate())
                .writeTo(Sinks.noop());

        Assert.assertThrows(JetException.class, () -> jet.newJob(p, jobConfig));
    }


    static ClassLoader filteringClassloader() {
        return new FilteringClassLoader(singletonList(DummyPredicate.class.getName()), null);
    }

}
