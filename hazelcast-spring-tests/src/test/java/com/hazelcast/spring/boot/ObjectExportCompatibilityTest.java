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

package com.hazelcast.spring.boot;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spring.ConfigCreator;
import com.hazelcast.spring.CustomSpringExtension;
import com.hazelcast.spring.ExposeHazelcastObjects;
import com.hazelcast.spring.boot.fortest.MySpringService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests checking compatibility of object export / {@link com.hazelcast.spring.ExposeHazelcastObjects} with Spring core mechanisms:
 * - ConfigurationProperties
 * - Injecting into other beans
 * - Services exported
 * - XML can access Java-generated beans
 */
@TestPropertySource("classpath:test-params.properties")
public class ObjectExportCompatibilityTest {

    public record MyWrapperJustToDeclareABean(IMap<String, String> map) {
    }

    @SpringBootApplication(scanBasePackages = "com.hazelcast.spring.boot.fortest")
    @EnableConfigurationProperties(HzConfProps.class)
    public static class ObjectExportConfigurationBase {
        @Bean
        public Config config(HzConfProps props) {
            Config conf = ConfigCreator.createConfigWithStructures();
            conf.setClusterName(props.clusterName());
            conf.setInstanceName(props.instanceName());
            return conf;
        }

        /**
         * A bean that uses auto-created IMap to ensure users will still be able to rely on exposed beans.
         */
        @Bean
        @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
        MyWrapperJustToDeclareABean myWrapperJustToDeclareABean(@Qualifier("testMap") IMap<String, String> map) {
            return new MyWrapperJustToDeclareABean(map);
        }
    }

    public abstract static class BaseObjectExportTest extends BaseBootTest {

        @Autowired
        private MyWrapperJustToDeclareABean myWrapperJustToDeclareABean;

        @Autowired
        @Qualifier("testMap")
        protected IMap<String, String> map;

        @Autowired
        protected MySpringService mySpringService;

        @Test
        void testInstancesConfigurationMatchesProperties() {
            assertThat(instance).anySatisfy(instance -> {
                assertThat(instance.getName()).isEqualTo("green-one");
                assertThat(instance.getConfig().getClusterName()).isEqualTo("rebel-alliance");
            });
        }

        @Test
        void canUseExposedStructuresInOtherBeans() {
            assertThat(myWrapperJustToDeclareABean).isNotNull();
            assertThat((Object) myWrapperJustToDeclareABean.map()).isNotNull();
            assertThat(myWrapperJustToDeclareABean.map().getName()).isEqualTo("testMap");

            assertThat(mySpringService.testMap.getName()).isEqualTo("testMap");
            assertThat(mySpringService.getISet().getName()).isEqualTo("setForTests");
            assertThatCode(() -> mySpringService.testMap.put("1", "1")).doesNotThrowAnyException();
            assertThatCode(() -> mySpringService.getISet().add("1")).doesNotThrowAnyException();

            assertThat(map.getName()).isEqualTo("testMap");
        }
    }

    @Nested
    @SpringBootTest(classes = ObjectExportCompatibilityTest.WithCustomExportConf.Configuration.class)
    @ExtendWith({SpringExtension.class, CustomSpringExtension.class})
    @DisplayName("with customized @ExposeHazelcastObjects parameters")
    public class WithCustomExportConf extends BaseObjectExportTest {

        @Test
        void excludedList() {
            assertThatThrownBy(() -> applicationContext.getBean(Ringbuffer.class))
                    .isInstanceOf(NoSuchBeanDefinitionException.class);
        }

        @ExposeHazelcastObjects(excludeByType = Ringbuffer.class)
        @SpringBootApplication(scanBasePackages = "com.hazelcast.spring.boot.fortest")
        @EnableConfigurationProperties(HzConfProps.class)
        public static class Configuration extends ObjectExportCompatibilityTest.ObjectExportConfigurationBase {
        }
    }

    @SpringBootTest(classes = EmptyExposeObjectAnnotation.Configuration.class)
    @ExtendWith({SpringExtension.class, CustomSpringExtension.class})
    @Nested
    @DisplayName("with empty @ExposeHazelcastObjects")
    public class EmptyExposeObjectAnnotation extends BaseObjectExportTest {

        @Test
        void nothingIsExcluded() {
            Ringbuffer<?> ringbuffer = applicationContext.getBean(Ringbuffer.class);
            assertThat(ringbuffer.getName()).isEqualTo("ringbuffer");
        }

        @ExposeHazelcastObjects
        @TestPropertySource("classpath:test-params.properties")
        public static class Configuration extends ObjectExportCompatibilityTest.ObjectExportConfigurationBase {
        }
    }

    @SpringBootTest(classes = WithXmlBeans.Configuration.class)
    @ExtendWith({SpringExtension.class, CustomSpringExtension.class})
    @Nested
    @DisplayName("with beans in XML file")
    public class WithXmlBeans extends BaseObjectExportTest {

        @Test
        void hasServiceFromXml() {
            PlainJavaService bean = applicationContext.getBean(PlainJavaService.class);
            assertThat(bean).isNotNull();
            assertThat(PlainJavaService.initDone).isTrue();
            assertThat((Object) bean.getTestMap()).isNotNull();
            assertThatCode(() -> bean.getTestMap().put("1", "1")).doesNotThrowAnyException();
        }

        @ExposeHazelcastObjects
        @TestPropertySource("classpath:test-params.properties")
        @ImportResource("classpath:com/hazelcast/spring/boot/context-using-java-generated-beans.xml")
        public static class Configuration extends ObjectExportCompatibilityTest.ObjectExportConfigurationBase {
        }
    }

    @SpringBootTest(classes = MultipleInstances.Configuration.class)
    @ExtendWith({SpringExtension.class, CustomSpringExtension.class})
    @Nested
    @DisplayName("with multiple instances")
    public class MultipleInstances extends BaseObjectExportTest {

        @ExposeHazelcastObjects
        @TestPropertySource("classpath:test-params.properties")
        public static class Configuration extends ObjectExportCompatibilityTest.ObjectExportConfigurationBase {
            @Bean
            public HazelcastInstance secondHazelcastInstance(HzConfProps props) {
                Config configWithStructures = ConfigCreator.createConfigWithStructures();
                configWithStructures.setInstanceName(props.instanceName());
                configWithStructures.setClusterName(props.clusterName());
                return Hazelcast.newHazelcastInstance(configWithStructures);
            }
            @Bean
            public HazelcastInstance thirdHazelcastInstance(HzConfProps props) {
                Config configWithStructures = ConfigCreator.createConfigWithStructures();
                configWithStructures.setClusterName(props.clusterName());
                return Hazelcast.newHazelcastInstance(configWithStructures);
            }
        }
    }

    @SpringBootTest(classes = MultipleInstancesDifferentClusters.Configuration.class)
    @ExtendWith({SpringExtension.class, CustomSpringExtension.class})
    @Nested
    @DisplayName("with multiple instances")
    public class MultipleInstancesDifferentClusters extends BaseObjectExportTest {

        @ExposeHazelcastObjects
        @TestPropertySource("classpath:test-params.properties")
        public static class Configuration extends ObjectExportCompatibilityTest.ObjectExportConfigurationBase {
            @Bean
            public HazelcastInstance secondHazelcastInstance(HzConfProps props) {
                Config configWithStructures = ConfigCreator.createConfigWithStructures();
                configWithStructures.setInstanceName(props.instanceName());
                configWithStructures.setClusterName(props.clusterName());
                return Hazelcast.newHazelcastInstance(configWithStructures);
            }
            @Bean
            public HazelcastInstance thirdHazelcastInstance() {
                Config configWithStructures = ConfigCreator.createConfig();
                configWithStructures.setClusterName("empire");
                return Hazelcast.newHazelcastInstance(configWithStructures);
            }
        }
    }

    @Nested
    @DisplayName("with unnamed instances in different clusters")
    public class UnnamedInstancesDifferentClusters {

        @Test
        void exportsMapsFromTheirOwnClusters() {
            try (var ac = new AnnotationConfigApplicationContext(Configuration.class)) {
                HazelcastInstance rebel = ac.getBean("rebel", HazelcastInstance.class);
                HazelcastInstance empire = ac.getBean("empire", HazelcastInstance.class);
                assertThat(rebel.getConfig().getInstanceName()).isNull();
                assertThat(empire.getConfig().getInstanceName()).isNull();
                assertThat(rebel.getConfig().getClusterName()).isNotEqualTo(empire.getConfig().getClusterName());

                IMap<?, ?> rebelMap = ac.getBean("rebelMap", IMap.class);
                IMap<?, ?> empireMap = ac.getBean("empireMap", IMap.class);
                assertThat((Object) rebelMap).isSameAs(rebel.getMap("rebelMap"));
                assertThat((Object) empireMap).isSameAs(empire.getMap("empireMap"));
            }
        }

        @ExposeHazelcastObjects
        public static class Configuration {

            @Bean
            public HazelcastInstance rebel() {
                Config config = ConfigCreator.createConfig();
                config.setClusterName("rebel-alliance");
                config.addMapConfig(new MapConfig("rebelMap"));
                return Hazelcast.newHazelcastInstance(config);
            }

            @Bean
            public HazelcastInstance empire() {
                Config config = ConfigCreator.createConfig();
                config.setClusterName("empire");
                config.addMapConfig(new MapConfig("empireMap"));
                return Hazelcast.newHazelcastInstance(config);
            }
        }
    }
}
