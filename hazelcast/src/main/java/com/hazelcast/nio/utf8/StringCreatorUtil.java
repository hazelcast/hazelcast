/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.utf8;

import com.hazelcast.nio.UTFUtil;
import com.hazelcast.util.JvmUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

public final class StringCreatorUtil {

    static final AtomicInteger CLASS_ID_COUNTER = new AtomicInteger();
    static final ClassLoader MAGIC_CLASSLOADER;
    static final Class<?> JAVA_LANG_ACCESS_CLASS;
    static final Class<?> SHARED_SECRET_CLASS;

    private static final boolean IS_IBM_JVM;
    private static final boolean IS_ORACLE_JVM;

    static {
        IS_IBM_JVM = JvmUtil.getJvmVendor() == JvmUtil.Vendor.IBM;
        IS_ORACLE_JVM = JvmUtil.getJvmVendor() == JvmUtil.Vendor.SunOracle;
        MAGIC_CLASSLOADER = findMagicClassLoader();
        JAVA_LANG_ACCESS_CLASS = findJavaLangAccess();
        SHARED_SECRET_CLASS = findSharedSecret();
    }

    private StringCreatorUtil() {
    }

    public static UTFUtil.StringCreator findBestStringCreator() {
        boolean faststringEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring", "true"));
        boolean java8Enabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring.java8", "true"));
        boolean asmEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring.asm", "true"));
        boolean bcelEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring.bcel", "true"));
        boolean internalBcelEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring.internalbcel", "true"));
        boolean javassistEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring.javassist", "true"));
        boolean debugEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring.debug", "false"));

        return findBestStringCreator(faststringEnabled, java8Enabled, asmEnabled,
                bcelEnabled, internalBcelEnabled, javassistEnabled, debugEnabled);
    }

    public static UTFUtil.StringCreator findBestStringCreator(boolean faststringEnabled, boolean java8Enabled,
                                                              boolean asmEnabled, boolean bcelEnabled,
                                                              boolean internalBcelEnabled, boolean javassistEnabled,
                                                              boolean debugEnabled) {
        if (!faststringEnabled) {
            return new DefaultStringCreator();
        }

        try {
            // Give access to the package private String constructor
            Constructor<String> constructor = null;
            if (useOldStringConstructor()) {
                constructor = String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            } else {
                constructor = String.class.getDeclaredConstructor(char[].class, boolean.class);
            }
            if (constructor != null) {
                constructor.setAccessible(true);
            }

            if (java8Enabled && isOracleJava8(debugEnabled)) {
                if ((internalBcelEnabled || bcelEnabled) && isBcelAvailable(debugEnabled)) {
                    boolean internal = internalBcelEnabled
                            && (isOracleBcelAvailable(debugEnabled) || isIBMBcelAvailable(debugEnabled));
                    UTFUtil.StringCreator stringCreator = tryLoadBcelJava8StringCreator(internal, debugEnabled);
                    if (stringCreator != null) {
                        return stringCreator;
                    }
                }

                if (asmEnabled && isAsmAvailable(debugEnabled)) {
                    UTFUtil.StringCreator stringCreator = tryLoadAsmJava8StringCreator(debugEnabled);
                    if (stringCreator != null) {
                        return stringCreator;
                    }
                }

                if (javassistEnabled && isJavassistAvailable(debugEnabled)) {
                    UTFUtil.StringCreator stringCreator = tryLoadJavassistJava8StringCreator(debugEnabled);
                    if (stringCreator != null) {
                        return stringCreator;
                    }
                }
            }

            if ((internalBcelEnabled || bcelEnabled) && isBcelAvailable(debugEnabled)) {
                boolean internal = internalBcelEnabled
                        && (isOracleBcelAvailable(debugEnabled) || isIBMBcelAvailable(debugEnabled));
                UTFUtil.StringCreator stringCreator = tryLoadBcelMagicAccessorStringCreator(internal, debugEnabled);
                if (stringCreator != null) {
                    return stringCreator;
                }
            }

            if (asmEnabled && isAsmAvailable(debugEnabled)) {
                UTFUtil.StringCreator stringCreator = tryLoadAsmMagicAccessorStringCreator(debugEnabled);
                if (stringCreator != null) {
                    return stringCreator;
                }
            }

            if (javassistEnabled && isJavassistAvailable(debugEnabled)) {
                UTFUtil.StringCreator stringCreator = tryLoadJavassistMagicAccessorStringCreator(debugEnabled);
                if (stringCreator != null) {
                    return stringCreator;
                }
            }

            // If bytecode generation is not possible use reflection
            return new FastStringCreator(constructor);

        } catch (Throwable ignore) {
            if (debugEnabled) {
                ignore.printStackTrace();
            }
        }

        // If everything else goes wrong just use default
        return new DefaultStringCreator();
    }

    static boolean useOldStringConstructor() {
        try {
            Class<String> clazz = String.class;
            Constructor<String> c = clazz.getDeclaredConstructor(int.class, int.class, char[].class);
            return true;
        } catch (Throwable ignore) {
        }
        return false;
    }

    private static boolean isOracleJava8(boolean debugEnabled) {
        try {
            Class<?> clazz = Class.forName("sun.misc.JavaLangAccess");
            Method method = clazz.getDeclaredMethod("newStringUnsafe", char[].class);
            if (method.getReturnType().equals(String.class)) {
                return true;
            }
        } catch (Throwable ignore) {
            if (debugEnabled) {
                ignore.printStackTrace();
            }
        }
        return false;
    }

    private static UTFUtil.StringCreator tryLoadAsmJava8StringCreator(boolean debugEnabled) {
        return loadStringCreator("com.hazelcast.nio.utf8.AsmJava8JlaStringCreatorBuilder", debugEnabled);
    }

    private static UTFUtil.StringCreator tryLoadAsmMagicAccessorStringCreator(boolean debugEnabled) {
        return loadStringCreator("com.hazelcast.nio.utf8.AsmMagicAccessorStringCreatorBuilder", debugEnabled);
    }

    private static boolean isAsmAvailable(boolean debugEnabled) {
        return isClassAvailable("org.objectweb.asm.ClassWriter", debugEnabled);
    }

    private static UTFUtil.StringCreator tryLoadJavassistJava8StringCreator(boolean debugEnabled) {
        return loadStringCreator("com.hazelcast.nio.utf8.JavassistJava8JlaStringCreatorBuilder", debugEnabled);
    }

    private static UTFUtil.StringCreator tryLoadJavassistMagicAccessorStringCreator(boolean debugEnabled) {
        return loadStringCreator("com.hazelcast.nio.utf8.JavassistMagicAccessorStringCreatorBuilder", debugEnabled);
    }

    private static boolean isJavassistAvailable(boolean debugEnabled) {
        return isClassAvailable("javassist.bytecode.ClassFileWriter", debugEnabled);
    }

    private static UTFUtil.StringCreator tryLoadBcelJava8StringCreator(boolean internal, boolean debugEnabled) {
        String classname;
        if (internal) {
            classname = "com.hazelcast.nio.utf8.OracleBcelJava8JlaStringCreatorBuilder";
        } else {
            classname = "com.hazelcast.nio.utf8.BcelJava8JlaStringCreatorBuilder";
        }
        return loadStringCreator(classname, debugEnabled);
    }

    private static UTFUtil.StringCreator tryLoadBcelMagicAccessorStringCreator(boolean internal, boolean debugEnabled) {
        String classname;
        if (internal) {
            if (IS_ORACLE_JVM) {
                classname = "com.hazelcast.nio.utf8.OracleBcelMagicAccessorStringCreatorBuilder";
            } else {
                classname = "com.hazelcast.nio.utf8.IBMBcelMagicAccessorStringCreatorBuilder";
            }
        } else {
            classname = "com.hazelcast.nio.utf8.BcelMagicAccessorStringCreatorBuilder";
        }
        return loadStringCreator(classname, debugEnabled);
    }

    private static boolean isBcelAvailable(boolean debugEnabled) {
        return isOracleBcelAvailable(debugEnabled)
                || isIBMBcelAvailable(debugEnabled)
                || isExternalBcelAvailable(debugEnabled);
    }

    private static boolean isOracleBcelAvailable(boolean debugEnabled) {
        return isClassAvailable("com.sun.org.apache.bcel.internal.generic.ClassGen", debugEnabled);
    }

    private static boolean isIBMBcelAvailable(boolean debugEnabled) {
        return IS_IBM_JVM && isClassAvailable("com.ibm.xtq.bcel.generic.ClassGen", debugEnabled);
    }

    private static boolean isExternalBcelAvailable(boolean debugEnabled) {
        return IS_ORACLE_JVM && isClassAvailable("org.apache.bcel.generic.ClassGen", debugEnabled);
    }

    private static boolean isClassAvailable(String classname, boolean debugEnabled) {
        try {
            Class.forName(classname);
            return true;
        } catch (Throwable ignore) {
            if (debugEnabled) {
                ignore.printStackTrace();
            }
        }
        return false;
    }

    private static UTFUtil.StringCreator loadStringCreator(String classname, boolean debugEnabled) {
        try {
            Class<?> builderClass = Class.forName(classname);
            StringCreatorBuilder builder = (StringCreatorBuilder) builderClass.newInstance();
            UTFUtil.StringCreator stringCreator = builder.build();
            if (debugEnabled && stringCreator != null) {
                System.err.println("StringCreator " + classname + " selected");
            }
            return stringCreator;
        } catch (Throwable ignore) {
            if (debugEnabled) {
                ignore.printStackTrace();
            }
        }
        return null;
    }

    private static ClassLoader findMagicClassLoader() {
        try {
            Class<?> clazz = Class.forName("sun.reflect.ConstructorAccessor");
            return clazz.getClassLoader();
        } catch (Throwable ignore) {
        }
        return null;
    }

    private static Class<?> findJavaLangAccess() {
        try {
            return Class.forName("sun.misc.JavaLangAccess");
        } catch (Throwable ignore) {
        }
        return null;
    }

    private static Class<?> findSharedSecret() {
        try {
            return Class.forName("sun.misc.SharedSecrets");
        } catch (Throwable ignore) {
        }
        return null;
    }

    private static class DefaultStringCreator implements UTFUtil.StringCreator {
        @Override
        public String buildString(char[] chars) {
            return new String(chars);
        }
    }

    private static class FastStringCreator implements UTFUtil.StringCreator {

        private final Constructor<String> constructor;

        private FastStringCreator(Constructor<String> constructor) {
            this.constructor = constructor;
        }

        @Override
        public String buildString(char[] chars) {
            try {
                if (useOldStringConstructor()) {
                    return constructor.newInstance(0, chars.length, chars);
                } else {
                    return constructor.newInstance(chars, Boolean.TRUE);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
