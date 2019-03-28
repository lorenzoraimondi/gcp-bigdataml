package gcp.cm.bigdata.adtech.dataflow;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProviders;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.json.JSONObject;

import javax.annotation.Nullable;

public class StreamingPipeline {

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(args[0]);
        options.setStagingLocation(args[1]);
        options.setTempLocation(args[2]);
        options.setRunner(DataflowRunner.class);
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);
        CoderRegistry cr = p.getCoderRegistry();
        cr.registerCoderForClass(JSONObject.class, new JSONObjectCoder());
        PCollection<JSONObject> coll = p
                .apply("ReadFromPubsub",
                        PubsubIO.readMessagesWithAttributes().fromTopic("projects/" + args[0] + "/topics/ad-impressions"))
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
                        .withoutSharding()
                        .withTempDirectory(FileSystems.matchNewResource("gs://bucket-for-codemotion/dataflow/device", false))
                        .to(DefaultFilenamePolicy.fromStandardParameters(ValueProvider.StaticValueProvider.of(FileSystems.matchNewResource("gs://bucket-for-codemotion/dataflow/site", false)), DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE, ".csv", true)))
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
                        .via((KV<String, Integer> kv) -> String.format("Site %s => %s", kv.getKey(), kv.getValue()))
                )
                .apply(TextIO.write()
                        .withWindowedWrites()
                        .withoutSharding()
                        .withTempDirectory(FileSystems.matchNewResource("gs://bucket-for-codemotion/dataflow/device", false))
                        .to(DefaultFilenamePolicy.fromStandardParameters(ValueProvider.StaticValueProvider.of(FileSystems.matchNewResource("gs://bucket-for-codemotion/dataflow/device", false)), DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE, ".csv", true)))
        ;

        p.run().waitUntilFinish();
    }

}
