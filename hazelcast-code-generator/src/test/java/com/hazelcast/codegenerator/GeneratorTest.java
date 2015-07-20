/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.codegenerator;

import com.hazelcast.client.protocol.generator.CodecCodeGenerator;
import com.hazelcast.client.protocol.generator.CodecModel;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class GeneratorTest {
    private Template template;

    public static void main(String[] args) {
        new GeneratorTest().generate();

    }

    public GeneratorTest() {
        Configuration cfg = new Configuration(Configuration.VERSION_2_3_23);
        cfg.setTemplateLoader(new ClassTemplateLoader(getClass(), "/"));
        try {
            cfg.setSetting("object_wrapper","DefaultObjectWrapper(2.3.23)");
            template = cfg.getTemplate("codec-template-java.ftl");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void generate() {
        try {
            CodecModel model = new CodecModel(true);
            Map<String, Object> data = new HashMap();
            CodecCodeGenerator.setUtilModel(data);
            data.put("model", model);
            StringWriter writer = new StringWriter();
            template.process(data, writer);
            String content = writer.toString();

            if( content == null || content.length() == 0) {
                content = ">>>>>>>>CONTENT EMPTY<<<<<<<<<<";

            }
            System.out.println(content);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
