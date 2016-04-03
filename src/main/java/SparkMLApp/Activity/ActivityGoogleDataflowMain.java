package SparkMLApp.Activity;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by shashi on 25/01/16.
 * usage:
 * spark-submit \
 --class SparkMLApp.Activity.ActivityGoogleDataflowMain \
 --master local \
 --jars $(echo /Users/shashi/code/SparkMLApp/target/lib/*.jar | tr ' ' ',') \
 /Users/shashi/code/SparkMLApp/target/SparkMLAppl-1.0-SNAPSHOT.jar \
 --inputFile=/Users/shashi/code/SparkMLApp/data/data2.csv  --output=/tmp/out --runner=SparkPipelineRunner --sparkMaster=local
 */

public class ActivityGoogleDataflowMain {


    static class ParseEventFn extends DoFn<String, Activity> {

        // Log and count parse errors.
        private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
        private final Aggregator<Long, Long> numParseErrors =
                createAggregator("ParseErrors", new Sum.SumLongFn());

        @Override
        public void processElement(ProcessContext c) {
            String[] components = c.element().split(",");
            try {
                String user =components[0];
                String activity =components[1];
                String timestamp =components[2];
                double xaxis =Double.parseDouble(components[3]);
                double yaxis =Double.parseDouble(components[4]);
                double zaxis =Double.parseDouble(components[5]);
                Activity gInfo = new Activity( user,  activity,  timestamp,  xaxis, yaxis,  zaxis);
                c.output(gInfo);
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                numParseErrors.addValue(1L);
                LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
            }
        }
    }


    public static class ExtractAndSumXaxis
            extends PTransform<PCollection<Activity>, PCollection<KV<String, Double>>> {

        private final String field;

        ExtractAndSumXaxis(String field) {
            this.field = field;
        }

        @Override
        public PCollection<KV<String, Double>> apply(
                PCollection<Activity> activityInfo) {

            return activityInfo
                    .apply(MapElements
                            .via((Activity gInfo) -> KV.of(gInfo.getActivity(), gInfo.getXaxis()))
                            .withOutputType(new TypeDescriptor<KV<String, Double>>() {}))
                    .apply(Sum.<String>doublesPerKey());
        }
    }


    public static interface ActivityCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();
        void setOutput(String value);

        public static class OutputFactory implements DefaultValueFactory<String> {
            @Override
            public String create(PipelineOptions options) {
                DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
                if (dataflowOptions.getStagingLocation() != null) {
                    return GcsPath.fromUri(dataflowOptions.getStagingLocation())
                            .resolve("counts.txt").toString();
                } else {
                    throw new IllegalArgumentException("Must specify --output or --stagingLocation");
                }
            }
        }

    }

    public static void main(String[] args) {
        ActivityCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(ActivityCountOptions.class);
        Pipeline p = Pipeline.create(options);


        p.apply(TextIO.Read.named("ReadActivity").from(options.getInputFile()))
                .apply(ParDo.named("ParseActivity").of(new ParseEventFn()))
                .apply("ExtractAndSumXaxis", new ExtractAndSumXaxis("user"))
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Double>, String>() {
            @Override
            public String apply(KV<String, Double> input) {
                return input.getKey() + ": " + input.getValue();
            }
        }))
                .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

        p.run();
    }
}
