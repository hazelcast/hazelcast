/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.server.JetBootstrap;

public class StartJet {
    static void s1() {
        //tag::s1[]
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        //end::s1[]
    }

    //tag::s2[]
    public static void main(String[] args) {
        try {
            JetInstance jet = Jet.newJetInstance();
            Jet.newJetInstance();

            // work with Jet

        } finally {
            Jet.shutdownAll();
        }
    }
    //end::s2[]


    static
    //tag::s3[]
    class JetExample {
        static Job createJob(JetInstance jet) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.addClass(JetExample.class);
            return jet.newJob(buildPipeline(), jobConfig);
        }

        static Pipeline buildPipeline() {
            Pipeline p = Pipeline.create();
            // ...
            return p;
        }
    }
    //end::s3[]

    static
    //tag::s4[]
    class CustomJetJob {
        public static void main(String[] args) {
            JetInstance jet = JetBootstrap.getInstance();
            jet.newJob(buildPipeline()).join();
        }

        static Pipeline buildPipeline() {
            Pipeline p = Pipeline.create();
            // ...
            return p;
        }
    }
    //end::s4[]
}
