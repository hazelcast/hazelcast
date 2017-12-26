package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class ComplexPrimitiveRecord implements Serializable {
    public boolean _boolean;
    public byte _byte;
    public short _short;
    public char _char;
    public int _int;
    public long _long;
    public float _float;
    public double _double;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComplexPrimitiveRecord that = (ComplexPrimitiveRecord) o;

        if (_boolean != that._boolean) return false;
        if (_byte != that._byte) return false;
        if (_short != that._short) return false;
        if (_char != that._char) return false;
        if (_int != that._int) return false;
        if (_long != that._long) return false;
        if (Float.compare(that._float, _float) != 0) return false;
        return Double.compare(that._double, _double) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = (_boolean ? 1 : 0);
        result = 31 * result + (int) _byte;
        result = 31 * result + (int) _short;
        result = 31 * result + (int) _char;
        result = 31 * result + _int;
        result = 31 * result + (int) (_long ^ (_long >>> 32));
        result = 31 * result + (_float != +0.0f ? Float.floatToIntBits(_float) : 0);
        temp = Double.doubleToLongBits(_double);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ComplexPrimitiveRecord{" +
                "_boolean=" + _boolean +
                ", _byte=" + _byte +
                ", _short=" + _short +
                ", _char=" + _char +
                ", _int=" + _int +
                ", _long=" + _long +
                ", _float=" + _float +
                ", _double=" + _double +
                '}';
    }
}
