package org.apache.hadoop.hive.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class DataSchemaDiscovery extends Configured implements Tool {

    public static class DataSchemaDiscoveryMapper
            extends Mapper<Object, Text, Text, HiveTypeWrapper> {

        private Text name = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject snapshot = (JsonObject) (new JsonParser()).parse(value.toString());
            JsonArray entities = snapshot.getAsJsonArray("entities");
            Iterator<JsonElement> it = entities.iterator();
            while (it.hasNext()) {
                JsonObject entity = (JsonObject) it.next();
                context.write(new Text(entity.get("entity_name").getAsString()), new HiveTypeWrapper(JsonSchemaFinder.pickType(entity)));
            }
        }
    }

    public static class DataSchemaDiscoveryReducer
            extends Reducer<Text, HiveTypeWrapper, Text, HiveTypeWrapper> {

        public void reduce(Text key, Iterable<HiveTypeWrapper> values,
                           Context context
        ) throws IOException, InterruptedException {
            HiveTypeWrapper result = null;
            for (HiveTypeWrapper type : values) {
                HiveType concreteType = null;
                if (result != null) {
                    concreteType = result.getInstance();
                }
                result = new HiveTypeWrapper(JsonSchemaFinder.mergeType(concreteType, type.getInstance()));
            }
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data schema discovery");
        job.setJarByClass(DataSchemaDiscovery.class);
        job.setMapperClass(DataSchemaDiscoveryMapper.class);
        job.setCombinerClass(DataSchemaDiscoveryReducer.class);
        job.setReducerClass(DataSchemaDiscoveryReducer.class);
        job.setOutputFormatClass(HiveTypeOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HiveTypeWrapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
        return (result ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DataSchemaDiscovery(), args);
    }

    public HiveType getHiveType() {
        return null;
    }
}
