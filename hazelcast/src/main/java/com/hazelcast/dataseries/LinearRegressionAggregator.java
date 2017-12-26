package com.hazelcast.dataseries;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.AbstractAggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

import static java.lang.Math.sqrt;

public class LinearRegressionAggregator
        extends AbstractAggregator<Object, Object, LinearRegression>
        implements DataSerializable {

    // could all be subject to overflow problems.
    public long sum_xy;
    public long sum_x;
    public long sum_y;
    public long sum_x_squared;
    public long sum_y_squared;

    // number of records
    public long n;

    public static double square(double d) {
        return d * d;
    }

    // every time we traverse a record, we need to call this.
    public void add(int x, int y) {
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_x_squared += x * x;
        sum_y_squared += y * y;
        n++;
    }

    // the calculations below are only done at the end

    public double a() {
        return mean_y() - b() * mean_x();
    }

    public double b() {
        return r() * standard_deviation_y() / standard_deviation_x();
    }

    public double r() {
        return (n * sum_xy - sum_x * sum_y) /
                sqrt((n * sum_x_squared - sum_x ^ 2) * (n * sum_y_squared - sum_y ^ 2));
    }

    private long mean_x() {
        return sum_x / n;
    }

    private long mean_y() {
        return sum_y / n;
    }

    // https://math.stackexchange.com/questions/198336/how-to-calculate-standard-deviation-with-streaming-inputs
    private double standard_deviation_x() {
        return standard_deviation(sum_x_squared, sum_x, n);
    }

    private double standard_deviation_y() {
        return standard_deviation(sum_y_squared, sum_y, n);
    }

    private static double standard_deviation(double sum_squared, double sum, double n) {
        return sqrt(sum_squared / n - square(sum / n));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(sum_x);
        out.writeLong(sum_y);
        out.writeLong(sum_xy);
        out.writeLong(sum_x_squared);
        out.writeLong(sum_y_squared);
        out.writeLong(n);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        sum_x = in.readLong();
        sum_y = in.readLong();
        sum_xy = in.readLong();
        sum_x_squared = in.readLong();
        sum_y_squared = in.readLong();
        n = in.readLong();
    }

    @Override
    protected void accumulateExtracted(Object entry, Object value) {
        throw new RuntimeException();
    }

    @Override
    public void combine(Aggregator a) {
        LinearRegressionAggregator aggregator = (LinearRegressionAggregator) a;

        sum_x += aggregator.sum_x;
        sum_y += aggregator.sum_y;
        sum_xy += aggregator.sum_xy;
        sum_x_squared += aggregator.sum_x_squared;
        sum_y_squared += aggregator.sum_y_squared;
        n += aggregator.n;
    }

    @Override
    public LinearRegression aggregate() {
        return new LinearRegression(r(), a(), b());
    }
}
