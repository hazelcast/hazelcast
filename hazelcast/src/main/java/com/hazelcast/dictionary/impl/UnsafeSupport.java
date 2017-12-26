package com.hazelcast.dictionary.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import static com.hazelcast.nio.Bits.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class UnsafeSupport {

    protected final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public final int arrayBaseOffset_boolean = unsafe.arrayBaseOffset(boolean[].class);
    public final int arrayBaseOffset_byte = unsafe.arrayBaseOffset(byte[].class);
    public final int arrayBaseOffset_short = unsafe.arrayBaseOffset(short[].class);
    public final int arrayBaseOffset_char = unsafe.arrayBaseOffset(char[].class);
    public final int arrayBaseOffset_int = unsafe.arrayBaseOffset(int[].class);
    public final int arrayBaseOffset_long = unsafe.arrayBaseOffset(long[].class);
    public final int arrayBaseOffset_float = unsafe.arrayBaseOffset(float[].class);
    public final int arrayBaseOffset_double = unsafe.arrayBaseOffset(double[].class);

    public final int arrayIndexScale_boolean = unsafe.arrayIndexScale(boolean[].class);
    public final int arrayIndexScale_byte = unsafe.arrayIndexScale(byte[].class);
    public final int arrayIndexScale_short = unsafe.arrayIndexScale(short[].class);
    public final int arrayIndexScale_char = unsafe.arrayIndexScale(char[].class);
    public final int arrayIndexScale_int = unsafe.arrayIndexScale(int[].class);
    public final int arrayIndexScale_long = unsafe.arrayIndexScale(long[].class);
    public final int arrayIndexScale_float = unsafe.arrayIndexScale(float[].class);
    public final int arrayIndexScale_double = unsafe.arrayIndexScale(double[].class);

    // boolean is  inefficient, it consumes 2 bytes; but can be done in 1 byte
    public void put_Boolean(long address, Object object, long fieldOffset) {
        Boolean fieldValue = (Boolean) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putBoolean(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Byte(long address, Object object, long fieldOffset) {
        Byte fieldValue = (Byte) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putByte(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Short(long address, Object object, long fieldOffset) {
        Short fieldValue = (Short) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putShort(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Character(long address, Object object, long fieldOffset) {
        Character fieldValue = (Character) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putChar(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Long(long address, Object object, long fieldOffset) {
        Long fieldValue = (Long) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putLong(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Integer(long address, Object object, long fieldOffset) {
        Integer fieldValue = (Integer) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putInt(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Float(long address, Object object, long fieldOffset) {
        Float fieldValue = (Float) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putFloat(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    public void put_Double(long address, Object object, long fieldOffset) {
        Double fieldValue = (Double) unsafe.getObject(object, fieldOffset);
        if (fieldValue == null) {
            unsafe.putBoolean(null, address, false);
        } else {
            unsafe.putBoolean(null, address, true);
            unsafe.putDouble(null, address + BYTE_SIZE_IN_BYTES, fieldValue);
        }
    }

    //todo: problem you need 2 return values; the bytes that have been read and position shift
    protected int unsafeGet_byteArray(long address, Object object, long fieldOffset) {
//        addLn("int length=unsafe.getInt(null, address+position);");
//        addLn("position+=INT_SIZE_IN_BYTES;");
//        addLn("if(length==-1){");
//        indent();
//        addLn("// ignore");
//        unindent();
//        addLn("} else {");
//        indent();
//        addLn("byte[] bytes = new byte[length];");
//        addLn("unsafe.copyMemory(null,address+position, bytes, unsafe.arrayBaseOffset(byte[].class), length);\n");
//        addLn("unsafe.putObject(value, %s, bytes);", field.offsetHeap());
//        addLn("position += length;");
//        unindent();
//        addLn("}");

//        int length = unsafe.getInt(address);
//        if(length==0)
//        byte[] array = (byte[]) unsafe.getObject(object, fieldOffset);
//        int written = INT_SIZE_IN_BYTES;
//        if (array == null) {
//            unsafe.putInt(null, address, -1);
//        } else {
//            unsafe.putInt(null, address, array.length);
//            int length = array.length * BOOLEAN_SIZE_IN_BYTES;
//            unsafe.copyMemory(array, unsafe.arrayBaseOffset(byte[].class),
//                    null, address + INT_SIZE_IN_BYTES, length);
//            written += length;
//        }
//
//        return written;
        throw new RuntimeException();
    }

    public int putArray_byte(long address, Object object, long fieldOffset) {
        byte[] array = (byte[]) unsafe.getObject(object, fieldOffset);
        return putArray_byte(address, array);
    }

    public int putArray_byte(long address, byte[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_boolean;
            unsafe.copyMemory(array, arrayBaseOffset_byte, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_boolean(long address, Object object, long fieldOffset) {
        boolean[] array = (boolean[]) unsafe.getObject(object, fieldOffset);
        return putArray_boolean(address, array);
    }

    public int putArray_boolean(long address, boolean[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_byte;
            unsafe.copyMemory(array, arrayBaseOffset_boolean, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_short(long address, Object object, long fieldOffset) {
        short[] array = (short[]) unsafe.getObject(object, fieldOffset);
        return putArray_short(address, array);
    }

    public int putArray_short(long address, short[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_short;
            unsafe.copyMemory(array, arrayBaseOffset_short, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_char(long address, Object object, long fieldOffset) {
        char[] array = (char[]) unsafe.getObject(object, fieldOffset);
        return putArray_char(address, array);
    }

    public int putArray_char(long address, char[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_char;
            unsafe.copyMemory(array, arrayBaseOffset_char, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_int(long address, Object object, long fieldOffset) {
        int[] array = (int[]) unsafe.getObject(object, fieldOffset);
        return putArray_int(address, array);
    }

    public int putArray_int(long address, int[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_int;
            unsafe.copyMemory(array, arrayBaseOffset_int, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_long(long address, Object object, long fieldOffset) {
        long[] array = (long[]) unsafe.getObject(object, fieldOffset);
        return putArray_long(address, array);
    }

    public int putArray_long(long address, long[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_long;
            unsafe.copyMemory(array, arrayBaseOffset_long, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_float(long address, Object object, long fieldOffset) {
        float[] array = (float[]) unsafe.getObject(object, fieldOffset);
        return putArray_float(address, array);
    }

    public int putArray_float(long address, float[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_float;
            unsafe.copyMemory(array, arrayBaseOffset_float, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }

    public int putArray_double(long address, Object object, long fieldOffset) {
        double[] array = (double[]) unsafe.getObject(object, fieldOffset);
        return putArray_double(address, array);
    }

    public int putArray_double(long address, double[] array) {
        int written = INT_SIZE_IN_BYTES;
        if (array == null) {
            unsafe.putInt(null, address, -1);
        } else {
            unsafe.putInt(null, address, array.length);
            int bytes = array.length * arrayIndexScale_double;
            unsafe.copyMemory(array, arrayBaseOffset_double, null, address + INT_SIZE_IN_BYTES, bytes);
            written += bytes;
        }

        return written;
    }
}
