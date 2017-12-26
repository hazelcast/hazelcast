package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class FieldAccessModifierTest extends HazelcastTestSupport {

    @Test
    public void whenPrivateField() {
        Dictionary<Long, PrivateField> dictionary = newDictionary(PrivateField.class);
        PrivateField p = new PrivateField();
        p.field = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void whenProtectedField() {
        Dictionary<Long, ProtectedField> dictionary = newDictionary(ProtectedField.class);
        ProtectedField p = new ProtectedField();
        p.field = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void whenPublicField() {
        Dictionary<Long, PublicField> dictionary = newDictionary(PublicField.class);
        PublicField p = new PublicField();
        p.field = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void whenPackagePrivate() {
        Dictionary<Long, PackagePrivateField> dictionary = newDictionary(PackagePrivateField.class);
        PackagePrivateField p = new PackagePrivateField();
        p.field = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void whenFinal() {
        Dictionary<Long, FinalField> dictionary = newDictionary(FinalField.class);
        FinalField p = new FinalField(10);

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    public static class PrivateField implements Serializable {
        private int field;


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PrivateField that = (PrivateField) o;

            return field == that.field;
        }

        @Override
        public int hashCode() {
            return field;
        }
    }

    public static class ProtectedField implements Serializable {
        protected int field;


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProtectedField that = (ProtectedField) o;

            return field == that.field;
        }

        @Override
        public int hashCode() {
            return field;
        }
    }

    public static class PublicField implements Serializable {
        public int field;


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PublicField that = (PublicField) o;

            return field == that.field;
        }

        @Override
        public int hashCode() {
            return field;
        }
    }

    public static class PackagePrivateField implements Serializable {
        int field;


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PackagePrivateField that = (PackagePrivateField) o;

            return field == that.field;
        }

        @Override
        public int hashCode() {
            return field;
        }
    }

    public static class FinalField implements Serializable {
        public final int field;

        public FinalField(int field) {
            this.field = field;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FinalField that = (FinalField) o;

            return field == that.field;
        }

        @Override
        public int hashCode() {
            return field;
        }
    }

    private <C> Dictionary<Long, C> newDictionary(Class<C> valueClass) {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(valueClass));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }
}
