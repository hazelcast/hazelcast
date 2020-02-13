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

import java.io.File;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;

import hex.genmodel.MojoModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.prediction.BinomialModelPrediction;;

public class MLModelPrediction {
    static void s1() {
        //tag::s1[]
        String modelFileLocation = "/path/to/gbm_model.zip";
        JetInstance jet = Jet.newJetInstance();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("H20 Classification Example");
        jobConfig.attachFile(modelFileLocation, "gbm_mojo");
        Job job = jet.newJob(buildPipeline(), jobConfig);
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        StreamStage<ItemToBeClassified> sourceStage = sourceStage();
        sourceStage.mapUsingService(ServiceFactories.sharedService(context -> {
            File modelFile = context.attachedFile("gbm_mojo");
            return new EasyPredictModelWrapper(MojoModel.load(modelFile.toString()));
        }), 
        (modelWrapper, item) -> {
            BinomialModelPrediction p = modelWrapper.predictBinomial(item.asRow());
            return p.label;
        }).setName("Apply H2O classification from loaded MOJO");
        //end::s2[]
    }

    static void s3() {
        //tag::s3[]
        //end::s3[]
    }

    static void s4() {
        //tag::s4[]
        //end::s4[]
    }

    static void s5() {
        //tag::s5[]
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

    static void s9() {
        //tag::s9[]
        //end::s9[]
    }

    static void s10() {
        //tag::s10[]
        //end::s10[]
    }

    static void s11() {
        //tag::s11[]
        //end::s11[]
    }
    
    private static Pipeline buildPipeline() {
        return null;
    }

    private static StreamStage<ItemToBeClassified> sourceStage() {
        return null;
    }

    private static class ItemToBeClassified {
        public RowData asRow() {
            return null;
        }

    }

}
