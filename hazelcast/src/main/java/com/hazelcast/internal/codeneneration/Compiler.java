/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.codeneneration;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.String.format;
import static java.security.AccessController.doPrivileged;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class Compiler {

    private final JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    private final File targetDirectory;
    private final ConcurrentMap<String, Class> classes = new ConcurrentHashMap<String, Class>();

    public Compiler(String name) {
        targetDirectory = new File(getUserDir(), name);
    }

    // synchronized is a hack; not very nice. but this is a poc.
    public synchronized Class compile(String className, String javacode) {
        ensureExistingDirectory(targetDirectory);

        Class clazz = classes.get(className);
        if (clazz == null) {
            JavaFileObject file = createJavaFileObject(className, javacode);
            clazz = compile(javaCompiler, file, className);
            classes.put(className, clazz);
        }
        return clazz;
    }

    public <E> Class<E> load(String className) {
        return classes.get(className);
    }

    private Class compile(JavaCompiler compiler, JavaFileObject file, final String className) {
        if (compiler == null) {
            throw new IllegalStateException("Could not get Java compiler."
                    + " You need to use a JDK! Version found: " + System.getProperty("java.version"));
        }

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                null,
                diagnostics,
                asList("-d", targetDirectory.getAbsolutePath()),
                null,
                singletonList(file));

        boolean success = task.call();
        if (!success) {
            StringBuilder sb = new StringBuilder();
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                sb.append("Error on line ")
                        .append(diagnostic.getLineNumber())
                        .append(" in ")
                        .append(diagnostic)
                        .append('\n');
            }
            throw new RuntimeException(sb.toString());
        }

        return (Class) doPrivileged((PrivilegedAction) () -> {
            try {
                URLClassLoader classLoader = new URLClassLoader(new URL[]{targetDirectory.toURI().toURL()});
                return (Class) classLoader.loadClass(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage(), e);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        });
    }

    private static File getUserDir() {
        return new File(System.getProperty("user.dir"));
    }

    private static File ensureExistingDirectory(File dir) {
        if (dir.isDirectory()) {
            return dir;
        }

        if (dir.isFile()) {
            throw new IllegalArgumentException(format("File [%s] is not a directory", dir.getAbsolutePath()));
        }

        // we don't care about the result because multiple threads are allowed to call this method concurrently
        // and therefore mkdirs() can return false if the directory has been created by another thread
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();

        // we just need to make sure the directory is created
        if (!dir.exists()) {
            throw new RuntimeException("Could not create directory: " + dir.getAbsolutePath());
        }

        return dir;
    }

    private JavaFileObject createJavaFileObject(String className, String javaCode) {
        try {
            File javaFile = new File(targetDirectory, className + ".java");
            writeText(javaCode, javaFile);
            return new JavaSourceFromString(className, javaCode);
        } catch (Exception e) {
            throw new RuntimeException(className + " ran into a code generation problem: " + e.getMessage(), e);
        }
    }

    private static void writeText(String text, File file) {
        FileOutputStream stream = null;
        OutputStreamWriter streamWriter = null;
        BufferedWriter writer = null;
        try {
            stream = new FileOutputStream(file);
            streamWriter = new OutputStreamWriter(stream);
            writer = new BufferedWriter(streamWriter);
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(writer);
            closeResource(streamWriter);
            closeResource(stream);
        }
    }

    private static class JavaSourceFromString extends SimpleJavaFileObject {

        private final String code;

        JavaSourceFromString(String name, String code) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }
}
