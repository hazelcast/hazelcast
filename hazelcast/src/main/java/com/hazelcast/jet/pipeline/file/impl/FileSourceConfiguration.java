/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline.file.impl;

import com.hazelcast.jet.pipeline.file.FileFormat;

import java.io.Serializable;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * File source configuration class containing parameters that are passed
 * to the factory.
 * <p>
 * This essentially carries all the file source parameters from the
 * builder to the {@link FileSourceFactory}.
 *
 * @param <T> type of items a source using this file format will emit
 */
public class FileSourceConfiguration<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final String glob;
    private final FileFormat<T> format;
    private final boolean sharedFileSystem;
    private final boolean ignoreFileNotFound;

    private final Map<String, String> options;

    /**
     * Create FileSourceConfiguration instance
     */
    public FileSourceConfiguration(String path, String glob, FileFormat<T> format,
                                   boolean sharedFileSystem, boolean ignoreFileNotFound, Map<String, String> options) {
        this.path = requireNonNull(path);
        this.glob = requireNonNull(glob);
        this.format = requireNonNull(format);
        this.sharedFileSystem = sharedFileSystem;
        this.ignoreFileNotFound = ignoreFileNotFound;
        this.options = requireNonNull(options);
    }

    /**
     * Returns the source directory path.
     */
    public String getPath() {
        return path;
    }

    /**
     * Returns the glob.
     */
    public String getGlob() {
        return glob;
    }

    /**
     * Returns the source format.
     */
    public FileFormat<T> getFormat() {
        return format;
    }

    /**
     * Returns if the filesystem is shared. Only valid for local filesystem, distributed filesystems are always shared.
     */
    public boolean isSharedFileSystem() {
        return sharedFileSystem;
    }

    /**
     * Returns if the Source should ignore no files at the location specified by the path and the glob
     */
    public boolean isIgnoreFileNotFound() {
        return ignoreFileNotFound;
    }

    /**
     * Returns the options configured for the file source.
     */
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "FileSourceConfiguration{" +
                "path='" + path + '\'' +
                ", glob='" + glob + '\'' +
                ", format=" + format +
                ", sharedFileSystem=" + sharedFileSystem +
                ", ignoreFileNotFound=" + ignoreFileNotFound +
                ", options=" + options +
                '}';
    }
}
