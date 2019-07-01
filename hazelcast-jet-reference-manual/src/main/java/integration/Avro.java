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

package integration;

import com.hazelcast.jet.avro.AvroSinks;
import com.hazelcast.jet.avro.AvroSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import datamodel.Person;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class Avro {

    static void s1() {
        //tag::s1[]
        Pipeline p = Pipeline.create();
        p.drawFrom(AvroSources.files("/home/jet/input", Person.class))
         .drainTo(Sinks.logger());
        //end::s1[]
    }

    static void s2() {
        //tag::s2[]
        String schemaString = "{\"type\":\"record\",\"name\":\"Person\"," +
                "\"namespace\":\"datamodel\",\"fields\":[{" +
                "\"name\":\"id\",\"type\":\"int\"}]}";
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<GenericRecord>list("inputList"))
         .drainTo(AvroSinks.files("/home/jet/output",
                 () -> new Schema.Parser().parse(schemaString)));
        //end::s2[]
    }
}
