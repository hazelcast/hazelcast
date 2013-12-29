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

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import org.apache.bcel.Constants;
import org.apache.bcel.classfile.*;
import org.apache.bcel.generic.ClassGen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CompileTimeRelocator {

    private static final String[][] RELOCATIONS = {
            {"com.hazelcast.nio.utf8.IBMBcelJava8JlaStringCreatorBuilder",
                    "org.apache.bcel.", "com.ibm.xtq.bcel." },
            {"com.hazelcast.nio.utf8.IBMBcelMagicAccessorStringCreatorBuilder",
                    "org.apache.bcel.", "com.ibm.xtq.bcel." },
            {"com.hazelcast.nio.utf8.OracleBcelJava8JlaStringCreatorBuilder",
                    "org.apache.bcel.", "com.sun.org.apache.bcel.internal." },
            {"com.hazelcast.nio.utf8.OracleBcelMagicAccessorStringCreatorBuilder",
                    "org.apache.bcel.", "com.sun.org.apache.bcel.internal." }
    };

    public static void main(String[] args) throws Exception {
        File baseDir = new File(args[0]);
        for (String[] relocation : RELOCATIONS) {
            String classname = relocation[0];
            String find = relocation[1];
            String replace = relocation[2];

            File path = new File(baseDir, "target/classes");
            String classPath = classname.replace(".", "/");
            File file = new File(path, classPath + ".class");

            System.out.print("Transforming " + file.getAbsolutePath() + "...");
            FileInputStream fis = new FileInputStream(file);
            execute(fis, classname, file, find, replace);
            System.out.println(" done.");
        }
    }

    private static void execute(InputStream is, String classname, File file,
                                String find, String replace) throws Exception {
        ClassParser cp = new ClassParser(is, file.getAbsolutePath().replace(".", "/") + ".class");
        JavaClass javaClass = cp.parse();
        ConstantPool pool = javaClass.getConstantPool();
        Constant[] constants = pool.getConstantPool();
        for (int i = 0; i < constants.length; i++) {
            Constant constant = constants[i];
            if (constant != null) {
                if (constant.getTag() == Constants.CONSTANT_Utf8) {
                    String data = pool.constantToString(constant);
                    if (data.contains(find)) {
                        pool.setConstant(i, new ConstantUtf8(replace(data, find, replace, false)));
                    } else if (data.contains(find.replace(".", "/"))) {
                        pool.setConstant(i, new ConstantUtf8(replace(data, find, replace, true)));
                    }
                }
            }
        }
        is.close();

        FileOutputStream fos = new FileOutputStream(file);
        javaClass.dump(fos);
        fos.close();
    }

    private static String replace(String base, String find, String replace, boolean internalType) {
        find = internalType ? find.replace(".", "/") : find;
        replace = internalType ? replace.replace(".", "/") : replace;

        Pattern pattern = Pattern.compile(find);
        Matcher matcher = pattern.matcher(base);
        StringBuilder result = new StringBuilder();

        int lastEnd = 0;
        while (matcher.find()) {
            result.append(base.substring(lastEnd, matcher.start()));
            result.append(replace);
            lastEnd = matcher.end();
        }
        result.append(base.substring(lastEnd));
        // System.out.println(base + " ==> " + result.toString());
        return result.toString();
    }

}
