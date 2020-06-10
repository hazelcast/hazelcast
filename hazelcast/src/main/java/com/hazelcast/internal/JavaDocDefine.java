/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Annotation to be used to relax checkstyle until the proper JavaDoc is
 * added to the annotated places.
 * <p/>
 * NOTE: This annotation is introduced only to enable turning on checks
 * for missing method annotations. This annotation is planned to be
 * removed eventually. If you see this annotation in a class you're
 * touching, please add the proper JavaDoc and remove the annotation.
 * <p/>
 * DO NOT ADD THIS ANNOTATION TO ANY METHOD OR TYPE
 * <p/>
 * For straightforward JavaDoc, consider using {@link JavaDocClear}.
 *
 * @see JavaDocClear
 * @deprecated It's better to write the JavaDoc.
 */
@Deprecated
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface JavaDocDefine {
}
