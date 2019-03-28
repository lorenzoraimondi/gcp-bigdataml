package gcp.cm.bigdata.adtech.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class CleaningPipeline {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);
        p
                .apply("ReadFromGCS",
//                        TextIO.read().from("gs://abucket-for-codemotion/adtech/test"))
                        TextIO.read().from("/Users/federico/Downloads/avazu-ctr-prediction/test"))
                .apply("SplitLines", MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((String line) -> Arrays.asList(line.split(",")))
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
//                        TextIO.write().withoutSharding().to("gs://abucket-for-codemotion/adtech/test-df-out.csv")
                        TextIO.write().withoutSharding().to("/Users/federico/Downloads/avazu-ctr-prediction/test-df-out.csv")
                )
        ;
        p.run().waitUntilFinish();
    }
}
