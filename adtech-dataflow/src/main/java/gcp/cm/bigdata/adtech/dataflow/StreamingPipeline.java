package gcp.cm.bigdata.adtech.dataflow;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Properties;

public class StreamingPipeline {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(StreamingPipeline.class.getResourceAsStream("/gcp.properties"));
        String project = props.getProperty("gcp.project-id");
        String bucket = props.getProperty("gcp.bucket");
        String bucketDest = String.format("gs://%s/%s", bucket, props.getProperty("streaming.write.folder"));

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(project);
        options.setStagingLocation(String.format("gs://%s/%s", bucket, props.getProperty("gcp.dataflow.staging")));
        options.setTempLocation(String.format("gs://%s/%s", bucket, props.getProperty("gcp.dataflow.temp")));
        options.setRunner(DataflowRunner.class);
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);
        CoderRegistry cr = p.getCoderRegistry();
        cr.registerCoderForClass(JSONObject.class, new JSONObjectCoder());
        PCollection<JSONObject> coll = p
                .apply("ReadFromPubsub",
                        PubsubIO.readMessagesWithAttributes().fromTopic("projects/" + project + "/topics/" + props.getProperty("gcp.pubsub.topic"))
//                        PubsubIO.readMessagesWithAttributes().fromSubscription("projects/" + project + "/subscriptions/" + props.getProperty("gcp.pubsub.subscription"))
                )
                .apply("ToJSON", MapElements
                        .into(TypeDescriptor.of(JSONObject.class))
                        .via((PubsubMessage msg) -> new JSONObject(new String(msg.getPayload())))
                )
                .apply("FilterNonClick", Filter.by((JSONObject obj) -> obj.getInt("clicked") > 0))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        ;

        coll.apply("SiteIdAndClicks", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((JSONObject obj) -> KV.of(obj.getString("siteId"), obj.getInt("clicked")))
                )
                .apply("SumSiteClicks",
                        Sum.integersPerKey()
                )
                .apply("MakeSiteStrings", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Integer> kv) -> String.format("Site %s => %s", kv.getKey(), kv.getValue()))
                )
                .apply(TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withTempDirectory(FileSystems.matchNewResource(bucketDest, true))
                        .to(DefaultFilenamePolicy.fromStandardParameters(ValueProvider.StaticValueProvider.of(FileSystems.matchNewResource(bucketDest + "site", false)), DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE, ".csv", true)))
        ;

        coll.apply("DeviceAndClicks", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((JSONObject obj) -> KV.of(obj.getString("deviceModel"), obj.getInt("clicked")))
                )
                .apply("SumDeviceClicks",
                        Sum.integersPerKey()
                )
                .apply("MakeDeviceStrings", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Integer> kv) -> String.format("Device %s => %s", kv.getKey(), kv.getValue()))
                )
                .apply(TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withTempDirectory(FileSystems.matchNewResource(bucketDest, true))
                        .to(DefaultFilenamePolicy.fromStandardParameters(ValueProvider.StaticValueProvider.of(FileSystems.matchNewResource(bucketDest + "device", false)), DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE, ".csv", true)))
        ;

        p.run().waitUntilFinish();
    }

}
