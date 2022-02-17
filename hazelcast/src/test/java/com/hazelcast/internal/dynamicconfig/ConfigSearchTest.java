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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.spi.properties.ClusterProperty.SEARCH_DYNAMIC_CONFIG_FIRST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigSearchTest extends HazelcastTestSupport {

    private static final String STATIC_NAME = "my.custom.data.*";
    private static final String DYNAMIC_NAME = "my.custom.data.cache";

    private HazelcastInstance hazelcastInstance;

    private void testTemplate(TestCase testCase) {
        Config staticHazelcastConfig = getConfig();
        if (testCase.isDynamicFirst()) {
            staticHazelcastConfig.setProperty(SEARCH_DYNAMIC_CONFIG_FIRST.getName(), "true");
        }
        String uuid = UUID.randomUUID().toString();
        staticHazelcastConfig.setInstanceName(uuid);
        staticHazelcastConfig.setClusterName(uuid);
        testCase.addStaticConfig(staticHazelcastConfig);

        hazelcastInstance = createHazelcastInstance(staticHazelcastConfig);
        testCase.addDynamicConfig(hazelcastInstance);

        testCase.asserts();
    }

    @Test
    public void testMapConfig_Static() {
        TestCase<MapConfig> testCase = new TestCase<MapConfig>(new MapConfig(STATIC_NAME), new MapConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MapConfig dataConfig = hazelcastInstance.getConfig().findMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testMapConfig_Dynamic() {
        TestCase<MapConfig> testCase = new TestCase<MapConfig>(new MapConfig(STATIC_NAME), new MapConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MapConfig dataConfig = hazelcastInstance.getConfig().findMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCacheConfig_Static() {
        TestCase<CacheSimpleConfig> testCase = new TestCase<CacheSimpleConfig>(new CacheSimpleConfig().setName(STATIC_NAME),
                new CacheSimpleConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addCacheConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addCacheConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CacheSimpleConfig dataConfig = hazelcastInstance.getConfig().findCacheConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCacheConfig_Dynamic() {
        TestCase<CacheSimpleConfig> testCase = new TestCase<CacheSimpleConfig>(new CacheSimpleConfig().setName(STATIC_NAME),
                new CacheSimpleConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addCacheConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addCacheConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CacheSimpleConfig dataConfig = hazelcastInstance.getConfig().findCacheConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testQueueConfig_Static() {
        TestCase<QueueConfig> testCase = new TestCase<QueueConfig>(new QueueConfig().setName(STATIC_NAME),
                new QueueConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addQueueConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addQueueConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                QueueConfig dataConfig = hazelcastInstance.getConfig().findQueueConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testQueueConfig_Dynamic() {
        TestCase<QueueConfig> testCase = new TestCase<QueueConfig>(new QueueConfig().setName(STATIC_NAME),
                new QueueConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addQueueConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addQueueConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                QueueConfig dataConfig = hazelcastInstance.getConfig().findQueueConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testListConfig_Static() {
        TestCase<ListConfig> testCase = new TestCase<ListConfig>(new ListConfig().setName(STATIC_NAME),
                new ListConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addListConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addListConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ListConfig dataConfig = hazelcastInstance.getConfig().findListConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testListConfig_Dynamic() {
        TestCase<ListConfig> testCase = new TestCase<ListConfig>(new ListConfig().setName(STATIC_NAME),
                new ListConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addListConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addListConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ListConfig dataConfig = hazelcastInstance.getConfig().findListConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testSetConfig_Static() {
        TestCase<SetConfig> testCase = new TestCase<SetConfig>(new SetConfig().setName(STATIC_NAME),
                new SetConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addSetConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addSetConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                SetConfig dataConfig = hazelcastInstance.getConfig().findSetConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testSetConfig_Dynamic() {
        TestCase<SetConfig> testCase = new TestCase<SetConfig>(new SetConfig().setName(STATIC_NAME),
                new SetConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addSetConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addSetConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                SetConfig dataConfig = hazelcastInstance.getConfig().findSetConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testMultiMapConfig_Static() {
        TestCase<MultiMapConfig> testCase = new TestCase<MultiMapConfig>(new MultiMapConfig().setName(STATIC_NAME),
                new MultiMapConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addMultiMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addMultiMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MultiMapConfig dataConfig = hazelcastInstance.getConfig().findMultiMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testMultiMapConfig_Dynamic() {
        TestCase<MultiMapConfig> testCase = new TestCase<MultiMapConfig>(new MultiMapConfig().setName(STATIC_NAME),
                new MultiMapConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addMultiMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addMultiMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                MultiMapConfig dataConfig = hazelcastInstance.getConfig().findMultiMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReplicatedMapConfig_Static() {
        TestCase<ReplicatedMapConfig> testCase = new TestCase<ReplicatedMapConfig>(new ReplicatedMapConfig().setName(STATIC_NAME),
                new ReplicatedMapConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addReplicatedMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addReplicatedMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReplicatedMapConfig dataConfig = hazelcastInstance.getConfig().findReplicatedMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReplicatedMapConfig_Dynamic() {
        TestCase<ReplicatedMapConfig> testCase = new TestCase<ReplicatedMapConfig>(new ReplicatedMapConfig().setName(STATIC_NAME),
                new ReplicatedMapConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addReplicatedMapConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addReplicatedMapConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReplicatedMapConfig dataConfig = hazelcastInstance.getConfig().findReplicatedMapConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testRingbufferConfig_Static() {
        TestCase<RingbufferConfig> testCase = new TestCase<RingbufferConfig>(new RingbufferConfig().setName(STATIC_NAME),
                new RingbufferConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addRingBufferConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addRingBufferConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                RingbufferConfig dataConfig = hazelcastInstance.getConfig().findRingbufferConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testRingbufferConfig_Dynamic() {
        TestCase<RingbufferConfig> testCase = new TestCase<RingbufferConfig>(new RingbufferConfig().setName(STATIC_NAME),
                new RingbufferConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addRingBufferConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addRingBufferConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                RingbufferConfig dataConfig = hazelcastInstance.getConfig().findRingbufferConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testTopicConfig_Static() {
        TestCase<TopicConfig> testCase = new TestCase<TopicConfig>(new TopicConfig().setName(STATIC_NAME),
                new TopicConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                TopicConfig dataConfig = hazelcastInstance.getConfig().findTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testTopicConfig_Dynamic() {
        TestCase<TopicConfig> testCase = new TestCase<TopicConfig>(new TopicConfig().setName(STATIC_NAME),
                new TopicConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                TopicConfig dataConfig = hazelcastInstance.getConfig().findTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableTopicConfig_Static() {
        TestCase<ReliableTopicConfig> testCase = new TestCase<ReliableTopicConfig>(new ReliableTopicConfig(STATIC_NAME),
                new ReliableTopicConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addReliableTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addReliableTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReliableTopicConfig dataConfig = hazelcastInstance.getConfig().findReliableTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableTopicConfig_Dynamic() {
        TestCase<ReliableTopicConfig> testCase = new TestCase<ReliableTopicConfig>(new ReliableTopicConfig(STATIC_NAME),
                new ReliableTopicConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addReliableTopicConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addReliableTopicConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ReliableTopicConfig dataConfig = hazelcastInstance.getConfig().findReliableTopicConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testExecutorConfig_Static() {
        TestCase<ExecutorConfig> testCase = new TestCase<ExecutorConfig>(new ExecutorConfig(STATIC_NAME),
                new ExecutorConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ExecutorConfig dataConfig = hazelcastInstance.getConfig().findExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testExecutorConfig_Dynamic() {
        TestCase<ExecutorConfig> testCase = new TestCase<ExecutorConfig>(new ExecutorConfig(STATIC_NAME),
                new ExecutorConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ExecutorConfig dataConfig = hazelcastInstance.getConfig().findExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testDurableExecutorConfig_Static() {
        TestCase<DurableExecutorConfig> testCase = new TestCase<DurableExecutorConfig>(
                new DurableExecutorConfig().setName(STATIC_NAME),
                new DurableExecutorConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addDurableExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addDurableExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                DurableExecutorConfig dataConfig = hazelcastInstance.getConfig().findDurableExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testDurableExecutorConfig_Dynamic() {
        TestCase<DurableExecutorConfig> testCase = new TestCase<DurableExecutorConfig>(
                new DurableExecutorConfig().setName(STATIC_NAME), new DurableExecutorConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addDurableExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addDurableExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                DurableExecutorConfig dataConfig = hazelcastInstance.getConfig().findDurableExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testScheduledExecutorConfig_Static() {
        TestCase<ScheduledExecutorConfig> testCase = new TestCase<ScheduledExecutorConfig>(
                new ScheduledExecutorConfig(STATIC_NAME), new ScheduledExecutorConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addScheduledExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addScheduledExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ScheduledExecutorConfig dataConfig = hazelcastInstance.getConfig().findScheduledExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testScheduledExecutorConfig_Dynamic() {
        TestCase<ScheduledExecutorConfig> testCase = new TestCase<ScheduledExecutorConfig>(
                new ScheduledExecutorConfig(STATIC_NAME), new ScheduledExecutorConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addScheduledExecutorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addScheduledExecutorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                ScheduledExecutorConfig dataConfig = hazelcastInstance.getConfig().findScheduledExecutorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCardinalityEstimatorConfig_Static() {
        TestCase<CardinalityEstimatorConfig> testCase = new TestCase<CardinalityEstimatorConfig>(
                new CardinalityEstimatorConfig().setName(STATIC_NAME),
                new CardinalityEstimatorConfig().setName(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addCardinalityEstimatorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addCardinalityEstimatorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CardinalityEstimatorConfig dataConfig
                        = hazelcastInstance.getConfig().findCardinalityEstimatorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testCardinalityEstimatorConfig_Dynamic() {
        TestCase<CardinalityEstimatorConfig> testCase = new TestCase<CardinalityEstimatorConfig>(
                new CardinalityEstimatorConfig().setName(STATIC_NAME),
                new CardinalityEstimatorConfig().setName(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addCardinalityEstimatorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addCardinalityEstimatorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                CardinalityEstimatorConfig dataConfig
                        = hazelcastInstance.getConfig().findCardinalityEstimatorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableIdGeneratorConfig_Static() {
        TestCase<FlakeIdGeneratorConfig> testCase = new TestCase<FlakeIdGeneratorConfig>(new FlakeIdGeneratorConfig(STATIC_NAME),
                new FlakeIdGeneratorConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addFlakeIdGeneratorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addFlakeIdGeneratorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                FlakeIdGeneratorConfig dataConfig = hazelcastInstance.getConfig().findFlakeIdGeneratorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testReliableIdGeneratorConfig_Dynamic() {
        TestCase<FlakeIdGeneratorConfig> testCase = new TestCase<FlakeIdGeneratorConfig>(new FlakeIdGeneratorConfig(STATIC_NAME),
                new FlakeIdGeneratorConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addFlakeIdGeneratorConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addFlakeIdGeneratorConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                FlakeIdGeneratorConfig dataConfig = hazelcastInstance.getConfig().findFlakeIdGeneratorConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testPNCounterConfig_Static() {
        TestCase<PNCounterConfig> testCase = new TestCase<PNCounterConfig>(new PNCounterConfig(STATIC_NAME),
                new PNCounterConfig(DYNAMIC_NAME), false) {
            @Override
            void addStaticConfig(Config config) {
                config.addPNCounterConfig(this.staticConfig);

            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addPNCounterConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                PNCounterConfig dataConfig = hazelcastInstance.getConfig().findPNCounterConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(STATIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    @Test
    public void testPNCounterConfig_Dynamic() {
        TestCase<PNCounterConfig> testCase = new TestCase<PNCounterConfig>(new PNCounterConfig(STATIC_NAME),
                new PNCounterConfig(DYNAMIC_NAME), true) {
            @Override
            void addStaticConfig(Config config) {
                config.addPNCounterConfig(this.staticConfig);
            }

            @Override
            void addDynamicConfig(HazelcastInstance hazelcastInstance) {
                hazelcastInstance.getConfig().addPNCounterConfig(this.dynamicConfig);
            }

            @Override
            void asserts() {
                PNCounterConfig dataConfig = hazelcastInstance.getConfig().findPNCounterConfig(DYNAMIC_NAME);
                assertThat(dataConfig.getName(), equalTo(DYNAMIC_NAME));
            }
        };
        testTemplate(testCase);
    }

    abstract class TestCase<T> {

        final T staticConfig;
        final T dynamicConfig;

        private final boolean isDynamicFirst;

        TestCase(T staticConfig, T dynamicConfig, boolean isDynamicFirst) {
            this.staticConfig = staticConfig;
            this.dynamicConfig = dynamicConfig;
            this.isDynamicFirst = isDynamicFirst;
        }

        boolean isDynamicFirst() {
            return isDynamicFirst;
        }

        abstract void addStaticConfig(Config config);

        abstract void addDynamicConfig(HazelcastInstance hazelcastInstance);

        abstract void asserts();
    }
}
