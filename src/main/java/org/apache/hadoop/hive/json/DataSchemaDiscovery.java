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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataSchemaDiscovery extends Configured implements Tool {

    public static class DataSchemaDiscoveryMapper
            extends Mapper<Object, Text, Text, HiveTypeWrapper> {

        private Text name = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject snapshot = (JsonObject) (new JsonParser()).parse(value.toString());

            String paramSkipEntities = context.getConfiguration().get("skipEntities");
            if (!"ALL".equals(paramSkipEntities)) {
                List<String> skipEntities = new ArrayList<>();
                if (paramSkipEntities != null && !paramSkipEntities.isEmpty()) {
                    String[] val = paramSkipEntities.split(",");
                    for (int i = 0; i< val.length; i++) {
                        skipEntities.add(val[i]);
                    }
                }

                JsonArray entities = snapshot.getAsJsonArray("entities");
                Iterator<JsonElement> it = entities.iterator();
                while (it.hasNext()) {
                    JsonObject entity = (JsonObject) it.next();
                    String entityName = entity.get("entity_name").getAsString();
                    // Add only if necessary
                    if (!skipEntities.contains(entityName)) {
                        context.write(new Text(entity.get("entity_name").getAsString()), new HiveTypeWrapper(JsonSchemaFinder.pickType(entity)));
                    }
                }
            }

            // Remove entities node and discovery all schema
            snapshot.remove("entities");
            // Process the entire JSON
            context.write(new Text("snapshot"), new HiveTypeWrapper(JsonSchemaFinder.pickType(snapshot)));
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
        conf.set("skipEntities", args[1]);
        Job job = Job.getInstance(conf, "Data schema discovery");
        job.setJarByClass(DataSchemaDiscovery.class);
        job.setMapperClass(DataSchemaDiscoveryMapper.class);
        job.setCombinerClass(DataSchemaDiscoveryReducer.class);
        job.setReducerClass(DataSchemaDiscoveryReducer.class);
        job.setOutputFormatClass(HiveTypeOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HiveTypeWrapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        for (int i = 2; i < args.length; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
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
