 /*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static com.hazelcast.jet.pipeline.Sinks.logger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

 @SuppressWarnings("NewClassNamingConvention")
public class SourceBuilder_ManagedContextTest extends HazelcastTestSupport {

    static class CleanupSavingContext implements Serializable {
        static volatile boolean created;
        static volatile boolean cleanedUp;
        CleanupSavingContext() {
            created = true;
        }

        void init() {
            cleanedUp = false;
            throw new RuntimeException("simulated fail");
        }

        void destroy() {
            cleanedUp = true;
        }
    }

    @Test
    public void cleansUpFromInit_in_batch_failInManagedContext() {
        var conf = smallInstanceConfig();
        conf.setManagedContext(new ThrowingManagedContext());
        var hz =  createHazelcastInstance(conf);

        // When
        BatchSource<String> source = SourceBuilder
                                         .batch("source", ctx -> new CleanupSavingContext())
                                         .initFn(CleanupSavingContext::init)
                                         .destroyFn(CleanupSavingContext::destroy)
                                         .<String>fillBufferFn((in, buf) -> {
                                             // not called
                                         })
                                         .build();

        // Then
        Pipeline p = Pipeline.create();
        p.readFrom(source)
         .writeTo(logger());

        assertThatCode(() -> hz.getJet().newJob(p).join())
            .hasMessageContaining("simulated fail");
        assertThat(CleanupSavingContext.created).isTrue();
        assertThat(CleanupSavingContext.cleanedUp).isTrue();
    }

    static final class ThrowingManagedContext implements ManagedContext {
        @Override
        public Object initialize(Object obj) {
            if (obj instanceof CleanupSavingContext) {
                throw new RuntimeException("simulated fail");
            }
            return obj;
        }
    }
}
