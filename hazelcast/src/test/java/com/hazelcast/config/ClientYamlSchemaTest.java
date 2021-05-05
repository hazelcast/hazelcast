package com.hazelcast.config;

import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlToJsonConverter;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaClient;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientYamlSchemaTest
        extends AbstractYamlSchemaTest {

    public static final Schema SCHEMA = SchemaLoader.builder()
            .schemaJson(readJSONObject("/hazelcast-config-5.0.json"))
            .draftV6Support()
            .schemaClient(SchemaClient.classPathAwareClient())
            .build()
            .load().build();

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> buildTestcases() {
        return Stream.concat(
                buildTestcases("com/hazelcast/config/yaml/testcases/client").stream(),
                buildTestcases("com/hazelcast/config/yaml/testcases/common").stream().map(ClientYamlSchemaTest::changeInputRootElem)
        ).collect(toList());
    }

    private static Object[] changeInputRootElem(Object[] args) {
        JSONObject input = (JSONObject) args[1];
        Object hazelcastValue = input.getJSONObject("hazelcast");
        input.remove("hazelcast");
        input.put("hazelcast-client", hazelcastValue);
        amendInstancePointersForClient((JSONObject) args[2]);
        return new Object[]{args[0], input, args[2]};
    }

    private static void amendInstancePointersForClient(JSONObject error) {
        if (error == null) {
            return;
        }
        String pointerToViolation = error.optString("pointerToViolation");
        if (pointerToViolation != null) {
            error.put("pointerToViolation", pointerToViolation.replace("#/hazelcast", "#/hazelcast-client"));
            error.optJSONArray("causingExceptions")
                    .forEach(causingExcJson -> amendInstancePointersForClient((JSONObject) causingExcJson));
        }
    }

    @Override
    protected Schema getSchema() {
        return SCHEMA;
    }
}
