package com.hazelcast.GB18030;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.GB18030.RandomIdeograph.generate;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.regex;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexComplianceTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Parameterized.Parameter(0)
    public IndexType indexType;

    @Parameterized.Parameters(name = "indexType: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {IndexType.BITMAP},
                {IndexType.HASH},
                {IndexType.SORTED},
        });
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_serializableIndexCompliance() {
        final String mapName = generate();
        Config config = configureMap(mapName, "随机");

        RandomSerializable serializable = new RandomSerializable();
        serializable.setRandom(generate());

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        IMap<Integer, RandomSerializable> map = instance.getMap(mapName);

        map.put(1, serializable);

        PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.get("随机").isNotNull();
        Collection<RandomSerializable> values = map.values(predicate);

        assertEquals(1, values.size());
        assertNotNull(map.get(1));
    }

    @Test
    public void test_DataSerializablePredicateCompliance() {
        final String mapName = generate();
        Config config = configureMap(mapName, "随");

        HazelcastInstance hazelcastInstance = hazelcastFactory.newHazelcastInstance(config);
        IMap<RandomDataSerializable, RandomDataSerializable> map = hazelcastInstance.getMap(mapName);

        Set<RandomDataSerializable> randoms = IntStream.rangeClosed(0, 42).boxed()
                .map(i -> new RandomDataSerializable())
                .peek(r -> map.put(r, r))
                .collect(Collectors.toSet());

        randoms.stream()
                .flatMap(r -> r.随机.stream())
                .forEach(r -> {
                    Predicate predicate = Predicates.equal("随机[any]", r);
                    Collection<RandomDataSerializable> result = map.values(predicate);
                    assertEquals(1, result.size());
                });

        randoms.forEach(r -> {
            RandomDataSerializable actual = map.get(r);
            assertEquals(r, actual);
        });
    }

    @Test
    public void test_JsonSerializableCompliance() {
        final String mapName = generate();
        Config config = configureMap(mapName, "年龄");

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(mapName);

        IntStream.rangeClosed(0, 42).boxed()
                .forEach(i -> {
                    String jsonString = "{\"年龄\":" + i + "}";
                    map.put(i, new HazelcastJsonValue(jsonString));
                });

        Predicate p = Predicates.equal("年龄", 42);
        assertEquals(1, map.values(p).size());
    }

    @Test
    public void test_genericRecordCompliance() {
        final String mapName = generate();
        Config config = configureMap(mapName, "名称");
        final int range = 42;

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance.getMap(mapName);

        // just to check no failure
        map.addIndex(indexType, "名称");

        IntStream.range(0, range).boxed().forEach(it -> {
            GenericRecord genericRecord = genericRecord();
            map.put(genericRecord, genericRecord);
        });

        int predicateOnValues = map.values(regex("名称", ".+")).size();
        int predicateOnKeys =
                map.keySet(Predicates.and(
                        regex("识别码", ".+"),
                        regex("名称", ".+"),
                        greaterThan("屏幕名称.识别码", 0))
                ).size();

        assertEquals(range, predicateOnValues);
        assertEquals(range, predicateOnKeys);
    }

    @Test
    public void test_portableCompliance() {
        final int range = 42;
        Config config = new Config();
        config.getSerializationConfig()
                .addPortableFactory(RandomPortable.FACTORY_ID, new RandomPortableFactory());
        config.getSerializationConfig()
                .addPortableFactory(InnerRandomPortable.FACTORY_ID, new RandomPortableFactory());

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(config);
        IMap<RandomPortable, RandomPortable> map = instance.getMap(generate());

        map.addIndex(indexType, "识别码");

        Set<RandomPortable> portables = IntStream.range(0, range).boxed().map(it -> {
            RandomPortable randomPortable = new RandomPortable();
            map.put(randomPortable, randomPortable);
            return randomPortable;
        }).collect(Collectors.toSet());

        portables.forEach(p -> {
            RandomPortable actual = map.get(p);
            assertEquals(p, actual);
        });

        portables.forEach(p -> {
            RandomPortable actual = map.values(Predicates.equal("识别码", p.识别码)).iterator().next();
            assertEquals(p, actual);
        });

        portables.forEach(p -> {
            int predicateOnKeys =
                    map.keySet(Predicates.and(
                            regex("识别码", ".+"),
                            regex("信息", ".+"),
                            regex("内.识别码", ".+"))
                    ).size();
            assertEquals(range, predicateOnKeys);
        });
    }

    private Config configureMap(String mapName, String attribute) {
        IndexConfig indexConfig = new IndexConfig();
        indexConfig.addAttribute(attribute);
        indexConfig.setType(indexType);

        MapConfig mapConfig = new MapConfig().setName(mapName);
        mapConfig.addIndexConfig(indexConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);
        return config;
    }

    static class RandomSerializable implements Serializable {
        private String 随机;

        RandomSerializable() {
        }

        String getRandom() {
            return 随机;
        }

        void setRandom(String random) {
            this.随机 = random;
        }
    }

    static class RandomDataSerializable implements DataSerializable {
        private String 随;
        private Collection<String> 随机;

        RandomDataSerializable() {
            随 = generate() + UUID.randomUUID().toString();
            随机 = IntStream.range(0, 42).boxed()
                    .map(it -> generate())
                    .collect(Collectors.toSet());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(随机);
            out.writeUTF(随);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            随机 = in.readObject();
            随 = in.readUTF();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RandomDataSerializable)) return false;
            RandomDataSerializable that = (RandomDataSerializable) o;
            return Objects.equals(随, that.随) &&
                    Objects.equals(随机, that.随机);
        }

        @Override
        public int hashCode() {
            return Objects.hash(随, 随机);
        }
    }

    class RandomPortableFactory implements PortableFactory {

        @Override
        public Portable create(int classId) {
            if (RandomPortable.CLASS_ID == classId) {
                return new RandomPortable();
            } else if (InnerRandomPortable.CLASS_ID == classId) {
                return new InnerRandomPortable();
            } else {
                return null;
            }
        }
    }

    static class RandomPortable implements Portable {
        public static final int CLASS_ID = 1;
        public static final int FACTORY_ID = 2;
        private String 识别码 = generate() + UUID.randomUUID().toString();
        private String 信息 = generate();
        private InnerRandomPortable 内 = new InnerRandomPortable();

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("识别码", 识别码);
            writer.writeUTF("信息", 信息);
            writer.writePortable("内", 内);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            识别码 = reader.readUTF("识别码");
            信息 = reader.readUTF("信息");
            内 = reader.readPortable("内");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RandomPortable)) return false;
            RandomPortable that = (RandomPortable) o;
            return Objects.equals(识别码, that.识别码) &&
                    Objects.equals(信息, that.信息) &&
                    Objects.equals(内, that.内);
        }

        @Override
        public int hashCode() {
            return Objects.hash(识别码, 信息, 内);
        }
    }

    static class InnerRandomPortable implements Portable {
        public static final int CLASS_ID = 2;
        public static final int FACTORY_ID = 3;
        private String 识别码 = generate() + UUID.randomUUID().toString();

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("识别码", 识别码);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            识别码 = reader.readUTF("识别码");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof InnerRandomPortable)) return false;
            InnerRandomPortable that = (InnerRandomPortable) o;
            return Objects.equals(识别码, that.识别码);
        }

        @Override
        public int hashCode() {
            return Objects.hash(识别码);
        }
    }

    private GenericRecord genericRecord() {
        ClassDefinition child = (new ClassDefinitionBuilder(1, 1))
                .addIntField("识别码")
                .addUTFField("名称")
                .build();

        ClassDefinition parent = (new ClassDefinitionBuilder(1, 2))
                .addUTFField("识别码")
                .addUTFField("名称")
                .addPortableField("屏幕名称", child)
                .build();

        return GenericRecord.Builder.portable(parent)
                .writeUTF("识别码", generate())
                .writeUTF("名称", generate())
                .writeGenericRecord("屏幕名称", GenericRecord.Builder.portable(child)
                        .writeInt("识别码", RandomUtils.nextInt(1, Integer.MAX_VALUE))
                        .writeUTF("名称", generate())
                        .build())
                .build();
    }

}
