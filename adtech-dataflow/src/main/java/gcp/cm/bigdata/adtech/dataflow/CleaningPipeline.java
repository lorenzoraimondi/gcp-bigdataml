package gcp.cm.bigdata.adtech.dataflow;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class CleaningPipeline {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(StreamingPipeline.class.getResourceAsStream("/gcp.properties"));
        String project = props.getProperty("gcp.project-id");
        String bucketSrc = props.getProperty("streaming.read.file");
        String bucketDest = props.getProperty("streaming.write.folder");

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(project);
        options.setStagingLocation(props.getProperty("gcp.dataflow.staging"));
        options.setTempLocation(props.getProperty("gcp.dataflow.temp"));
        options.setRunner(DataflowRunner.class);
        options.setStreaming(false);

        Pipeline p = Pipeline.create(options);
        p
                .apply("ReadFromGCS",
                        TextIO.read().from(bucketSrc))
                .apply("SplitLines", MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((String line) -> new ArrayList<>(Arrays.asList(line.split(","))))
                )
                .apply("CleanRecords", MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((List<String> fields) -> {
                            fields.remove(2);
                            fields.subList(0,11);
                            return fields;
                        })
                )
                .apply("BuildLines", MapElements
                        .into(TypeDescriptors.strings())
                        .via((List<String> fields) -> String.join(",", fields))
                )
                .apply("WriteToGCS",
                        TextIO.write().withoutSharding().to(bucketDest + "output-df.csv")
                )
        ;
        p.run().waitUntilFinish();
    }
}
