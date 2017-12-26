package com.hazelcast.dictionary.examples;

import java.io.Serializable;

public class ComplexRecord implements Serializable {
    public boolean _boolean;
    public byte _byte;
    public short _short;
    public char _char;
    public int _int;
    public long _long;
    public float _float;
    public double _double;
    public Boolean _Boolean;
    public Byte _Byte;
    public Short _Short;
    public Character _Character;
    public Integer _Integer;
    public Long _Long;
    public Float _Float;
    public Double _Double;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComplexRecord that = (ComplexRecord) o;

        if (_boolean != that._boolean) return false;
        if (_byte != that._byte) return false;
        if (_short != that._short) return false;
        if (_char != that._char) return false;
        if (_int != that._int) return false;
        if (_long != that._long) return false;
        if (Float.compare(that._float, _float) != 0) return false;
        if (Double.compare(that._double, _double) != 0) return false;
        if (_Boolean != null ? !_Boolean.equals(that._Boolean) : that._Boolean != null) return false;
        if (_Byte != null ? !_Byte.equals(that._Byte) : that._Byte != null) return false;
        if (_Short != null ? !_Short.equals(that._Short) : that._Short != null) return false;
        if (_Character != null ? !_Character.equals(that._Character) : that._Character != null) return false;
        if (_Integer != null ? !_Integer.equals(that._Integer) : that._Integer != null) return false;
        if (_Long != null ? !_Long.equals(that._Long) : that._Long != null) return false;
        if (_Float != null ? !_Float.equals(that._Float) : that._Float != null) return false;
        return _Double != null ? _Double.equals(that._Double) : that._Double == null;
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
        result = 31 * result + (_Boolean != null ? _Boolean.hashCode() : 0);
        result = 31 * result + (_Byte != null ? _Byte.hashCode() : 0);
        result = 31 * result + (_Short != null ? _Short.hashCode() : 0);
        result = 31 * result + (_Character != null ? _Character.hashCode() : 0);
        result = 31 * result + (_Integer != null ? _Integer.hashCode() : 0);
        result = 31 * result + (_Long != null ? _Long.hashCode() : 0);
        result = 31 * result + (_Float != null ? _Float.hashCode() : 0);
        result = 31 * result + (_Double != null ? _Double.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ComplexRecord{" +
                "_boolean=" + _boolean +
                ", _byte=" + _byte +
                ", _short=" + _short +
                ", _char=" + _char +
                ", _int=" + _int +
                ", _long=" + _long +
                ", _float=" + _float +
                ", _double=" + _double +
                ", _Boolean=" + _Boolean +
                ", _Byte=" + _Byte +
                ", _Short=" + _Short +
                ", _Character=" + _Character +
                ", _Integer=" + _Integer +
                ", _Long=" + _Long +
                ", _Float=" + _Float +
                ", _Double=" + _Double +
                '}';
    }
}
