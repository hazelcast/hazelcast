package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.TestCollectionUtils.setOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiValuePredicateTest extends HazelcastTestSupport {

    @Test
    public void testCollectionPredicate_andSQL() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Integer, Body> map = instance.getMap("map");

        Body body1 = new Body("body1")
                .addLimb(new Limb("ugly leg")
                                .addNail(new Nail("red"))
                                .addNail(new Nail("blue"))
                )
                .addLimb(new Limb("missing hand")
                                .addNail(new Nail("yellow"))
                );
        Body body2 = new Body("body2")
                .addLimb(new Limb("hook")
                        .addNail(new Nail("yellow"))
                        .addNail(new Nail("red")))
                .addLimb(new Limb("ugly leg"));

        Body body3 = new Body("body3")
                .insertStuffIntoPocket("left pocket", "beer");


        map.put(1, body1);
        map.put(3, body2);
        map.put(2, body3);

        SqlPredicate predicate = new SqlPredicate("limbs[*].name = 'missing hand'");
        Collection<Body> values = map.values(predicate);

        assertThat(values, contains(body1));
    }



        @Test
    public void testCollectionPredicate() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Integer, Body> map = instance.getMap("map");
        map.addIndex("limbs[0].name", false);
        map.addIndex("limbs[*].name", true);

        Body body1 = new Body("body1")
                .addLimb(new Limb("ugly leg")
                                .addNail(new Nail("red"))
                                .addNail(new Nail("blue"))
                )
                .addLimb(new Limb("missing hand")
                                .addNail(new Nail("yellow"))
                );


        Body body2 = new Body("body2")
                .addLimb(new Limb("hook")
                        .addNail(new Nail("yellow"))
                        .addNail(new Nail("red")))
                .addLimb(new Limb("ugly leg"));

        Body body3 = new Body("body3")
                .insertStuffIntoPocket("left pocket", "beer");


        map.put(1, body1);
        map.put(2, body2);
        map.put(3, body3);

        Predicate predicate = new EqualPredicate("limbs[*].nails[*].colour", "red");
        Collection<Body> values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new ContainsPredicate("pockets.values", "beer");
        values = map.values(predicate);
        assertThat(values, contains(body3));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[*].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[0].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[4].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, empty());
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new EqualPredicate("limbs[1].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, contains(body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new EqualPredicate("limbs[0].nails[*].colour", "yellow");
        values = map.values(predicate);
        assertThat(values, contains(body2));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[0].name", null);
        values = map.values(predicate);
        assertThat(values, contains(body3));
        for (Body body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


    }

    @Test
    public void testArrayPredicate() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Integer, ArrayBody> map = instance.getMap("map");
        map.addIndex("limbs[0].name", false);
        map.addIndex("limbs[*].name", true);

        ArrayBody body1 = new ArrayBody("body1",
                new ArrayLimb("ugly leg",
                        new Nail("red"),
                        new Nail("blue")
                ),
                new ArrayLimb("missing hand",
                        new Nail("yellow")
                )
        );


        ArrayBody body2 = new ArrayBody("body2",
                new ArrayLimb("hook",
                        new Nail("yellow"),
                        new Nail("red")
                ),
                new ArrayLimb("ugly leg")
        );

        ArrayBody body3 = new ArrayBody("body3");
        body3.insertStuffIntoPocket("left pocket", "beer");

        map.put(1, body1);
        map.put(2, body2);
        map.put(3, body3);

        Predicate predicate = new EqualPredicate("limbs[*].nails[*].colour", "red");
        Collection<ArrayBody> values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new ContainsPredicate("pockets.values", "beer");
        values = map.values(predicate);
        assertThat(values, contains(body3));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[*].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[0].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[4].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, empty());
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new EqualPredicate("limbs[1].name", "ugly leg");
        values = map.values(predicate);
        assertThat(values, contains(body2));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


        predicate = new EqualPredicate("limbs[0].nails[*].colour", "yellow");
        values = map.values(predicate);
        assertThat(values, contains(body2));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[0].name", null);
        values = map.values(predicate);
        assertThat(values, contains(body3));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");

        predicate = new EqualPredicate("limbs[*].nails[*].colour", "red");
        values = map.values(predicate);
        assertThat(values, containsInAnyOrder(body1, body2));
        for (ArrayBody body : values) {
            System.out.println(body);
        }
        System.out.println("-----");


    }

    private static class Body implements Serializable {
        private final String name;
        private Collection<Limb> limbs;
        private Map<String, String> pockets;

        public Body(String name) {
            this.pockets = new HashMap<String, String>();
            this.limbs = new ArrayList<Limb>();
            this.name = name;
        }

        public Body addLimb(Limb limb) {
            limbs.add(limb);
            return this;
        }

        public Body insertStuffIntoPocket(String pocket, String stuff) {
            pockets.put(pocket, stuff);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Body body = (Body) o;

            if (!name.equals(body.name)) return false;
            if (!limbs.equals(body.limbs)) return false;
            return pockets.equals(body.pockets);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + limbs.hashCode();
            result = 31 * result + pockets.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Body{" +
                    "name='" + name + '\'' +
                    ", limbs=" + limbs +
                    ", pockets=" + pockets +
                    '}';
        }
    }

    private static class Limb implements Serializable {
        private final String name;
        private final Collection<Nail> nails;

        public Limb(String name) {
            this.name = name;
            this.nails = new ArrayList<Nail>();
        }

        public Limb addNail(Nail nail) {
            nails.add(nail);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Limb limb = (Limb) o;

            if (!name.equals(limb.name)) return false;
            return nails.equals(limb.nails);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + nails.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Limb{" +
                    "name='" + name + '\'' +
                    ", nails=" + nails +
                    '}';
        }
    }


    private static class ArrayBody implements Serializable {
        private final String name;
        private ArrayLimb[] limbs;
        private Map<String, String> pockets;

        public ArrayBody(String name, ArrayLimb... limbs) {
            this.pockets = new HashMap<String, String>();
            this.name = name;
            this.limbs = limbs;
        }

        public ArrayBody insertStuffIntoPocket(String pocket, String stuff) {
            pockets.put(pocket, stuff);
            return this;
        }

        @Override
        public String toString() {
            return "Body{" +
                    "name='" + name + '\'' +
                    ", limbs=" + Arrays.toString(limbs) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArrayBody body = (ArrayBody) o;

            if (!name.equals(body.name)) return false;
            if (!Arrays.equals(limbs, body.limbs)) return false;
            return pockets.equals(body.pockets);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Arrays.hashCode(limbs);
            result = 31 * result + pockets.hashCode();
            return result;
        }
    }

    private static class ArrayLimb implements Serializable {
        private final String name;
        private final Nail[] nails;

        public ArrayLimb(String name, Nail... nails) {
            this.name = name;
            this.nails = nails;
        }

        @Override
        public String toString() {
            return "Limb{" +
                    "name='" + name + '\'' +
                    ", nails=" + Arrays.toString(nails) +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArrayLimb limb = (ArrayLimb) o;

            if (!name.equals(limb.name)) return false;
            return Arrays.equals(nails, limb.nails);

        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + Arrays.hashCode(nails);
            return result;
        }
    }

    private static class Nail implements Serializable {
        private final String colour;
        public Nail(String colour) {
            this.colour = colour;
        }

        @Override
        public String toString() {
            return "Nail{" +
                    "color='" + colour + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Nail nail = (Nail) o;

            return colour.equals(nail.colour);

        }

        @Override
        public int hashCode() {
            return colour.hashCode();
        }
    }
}
