package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Set;

import static com.hazelcast.mapreduce.aggregation.Aggregations.distinctValues;
import static com.hazelcast.mapreduce.aggregation.Supplier.fromPredicate;
import static com.hazelcast.query.Predicates.equal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DistinctAggregationTest extends AbstractAggregationTest {

    @Test
    public void testDistinctAggregationWithPredicates() {
        String mapName = randomMapName();
        IMap<Integer, Car> map = HAZELCAST_INSTANCE.getMap(mapName);

        Car vw1999 = Car.newCar(1999, "VW");
        Car bmw2000 = Car.newCar(2000, "BMW");
        Car vw2000 = Car.newCar(2000, "VW");

        map.put(0, vw1999);
        map.put(1, bmw2000);
        map.put(2, vw2000);

        Supplier<Integer, Car, Car> supplier = fromPredicate(equal("buildYear", 2000));
        Aggregation<Integer, Car, Set<Car>> aggregation = distinctValues();
        Set<Car> cars = map.aggregate(supplier, aggregation);

        assertThat(cars, containsInAnyOrder(bmw2000, vw2000));
    }

    private static class Car implements Serializable {
        private int buildYear;
        private String brand;

        private static Car newCar(int buildYear, String brand) {
            Car car = new Car();
            car.buildYear = buildYear;
            car.brand = brand;
            return car;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Car)) return false;

            Car car = (Car) o;

            if (buildYear != car.buildYear) return false;
            return brand.equals(car.brand);

        }

        @Override
        public int hashCode() {
            int result = buildYear;
            result = 31 * result + brand.hashCode();
            return result;
        }
    }
}
