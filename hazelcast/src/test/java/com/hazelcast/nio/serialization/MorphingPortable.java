package com.hazelcast.nio.serialization;

/**
 * Portable for version support tests
 */
public class MorphingPortable extends MorphingBasePortable implements VersionedPortable {

    public MorphingPortable(byte aByte, boolean aBoolean, char character, short aShort, int integer, long aLong, float aFloat,
            double aDouble, String aString) {
        super(aByte, aBoolean, character, aShort, integer, aLong, aFloat, aDouble, aString);
    }

    public MorphingPortable() {
    }

    @Override
    public int getClassVersion() {
        return 2;
    }

}
