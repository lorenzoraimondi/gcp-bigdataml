package gcp.cm.bigdata.adtech.dataflow;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.json.JSONObject;

import javax.xml.soap.Text;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamingPipeline {

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(args[0]);
        options.setStagingLocation(args[1]);
        options.setTempLocation(args[2]);
        options.setRunner(DataflowRunner.class);
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);
        PCollection<JSONObject> coll = p
                .apply("ReadFromPubsub",
                        PubsubIO.readMessagesWithAttributes().fromTopic("ad-impressions"))
                .apply("ToJSON", MapElements
                        .into(TypeDescriptor.of(JSONObject.class))
                        .via((PubsubMessage msg) -> new JSONObject(new String(msg.getPayload())))
                )
                .apply("FilterNonClick", Filter.by((JSONObject obj) -> obj.getInt("clicked") > 0))
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
                .apply(TextIO.write().withoutSharding().to("gs://abucket-for-codemotion/adtech/clicks-site-%s.csv"))
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
                .apply(TextIO.write().withoutSharding().to("gs://abucket-for-codemotion/adtech/clicks-device-%s.csv"))
        ;

        p.run().waitUntilFinish();
    }
}
