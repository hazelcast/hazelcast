package com.hazelcast.dataseries;

import java.io.Serializable;

public class LinearRegression implements Serializable {

    private final double r;
    private final double a;
    private final double b;

    public LinearRegression(double r, double a, double b) {
        this.r = r;
        this.a = a;
        this.b = b;
    }

    public double r() {
        return r;
    }

    // intercept
    public double a() {
        return a;
    }

    // this is the slope
    public double b() {
        return b;
    }
}
