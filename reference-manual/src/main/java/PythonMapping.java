import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.jet.python.PythonTransforms;

public class PythonMapping {
    public static void s1() {
        //tag::s1[]
        StreamStage<String> sourceStage = sourceStage();
        StreamStage<String> pythonMapped = sourceStage.apply(PythonTransforms.mapUsingPython(
                new PythonServiceConfig().setHandlerFile("path/to/handler.py")));
        //end::s1[]
        /*
        //tag::s2[]
        def transform_list(input_list):
            return ['reply-' + item for item in input_list]
        //end::s2[]
        */
    }

    private static void s3() {
        //tag::s3[]
        StreamStage<String> sourceStage = sourceStage();
        StreamStage<String> pythonMapped = sourceStage.apply(PythonTransforms.mapUsingPython(
                new PythonServiceConfig().setBaseDir("path/to/python_project")
                                         .setHandlerModule("jet_handler")));
        //end::s3[]

    }

    private static StreamStage<String> sourceStage() {
        return null;
    }
}
