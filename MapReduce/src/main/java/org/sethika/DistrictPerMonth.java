package org.sethika;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class DistrictPerMonth {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private HashMap<String, String> locationMap = new HashMap<>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("locationData.csv"));
                String line;
                reader.readLine(); // Skip header
                while ((line = reader.readLine()) != null) {
                    String[] columns = line.split(",");
                    String locationId = columns[0];
                    String city = columns[7];
                    locationMap.put(locationId, city);
                }
                reader.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("location_id")) {
                return;
            }

            try {
                String[] columns = line.split(",");
                String locationId = columns[0];
                String date = columns[1];
                double temp = Double.parseDouble(columns[5]);
                int precipitation = Integer.parseInt(columns[12]);

                String city = locationMap.getOrDefault(locationId, "Unknown");

                String[] dateParts = date.split("-");
                String year = dateParts[0];
                String month = dateParts[1];

                outputKey.set(city + "_" + year + "_" + month);
                outputValue.set(temp + "," + precipitation);

                context.write(outputKey, outputValue);
            } catch (Exception e) {
                System.out.println("Error processing line: " + e.getMessage());
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalTemp = 0.0;
            int totalHoursPrecipitation = 0;
            int count = 0;

            for (Text value : values) {
                String[] valueParts = value.toString().split(",");
                totalTemp += Double.parseDouble(valueParts[0]);
                totalHoursPrecipitation += Integer.parseInt(valueParts[1]);
                count++;
            }

            double avgTemp = Math.round((totalTemp / count) * 100.0) / 100.0;
            context.write(key, new Text(avgTemp + "," + totalHoursPrecipitation));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DistrictPerMonthWeather");
        job.setJarByClass(DistrictPerMonth.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI("hdfs://namenode:8020/data/processed_location_data.csv#locationData.csv"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}