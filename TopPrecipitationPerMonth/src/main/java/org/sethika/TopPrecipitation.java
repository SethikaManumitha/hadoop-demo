package org.sethika;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopPrecipitation {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private Text monthYearKey = new Text();
        private IntWritable precipitationValue = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("location_id") || line.trim().isEmpty()) return;

            String[] cols = line.split(",");
            try {
                String date = cols[1]; // "1/2/2010"
                int precipitationHours = Integer.parseInt(cols[12]);

                String[] parts = date.split("/"); // MM/DD/YYYY
                String month = parts[0].length() == 1 ? "0" + parts[0] : parts[0];
                String year = parts[2];
                monthYearKey.set(year + "-" + month);
                precipitationValue.set(precipitationHours);
                context.write(monthYearKey, precipitationValue);
            } catch (Exception e) {
                System.out.println("Error processing line: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private String maxMonthYear = "";
        private int maxTotal = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if (sum > maxTotal) {
                maxTotal = sum;
                maxMonthYear = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(maxMonthYear), new IntWritable(maxTotal));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Precipitation Month-Year");
        job.setJarByClass(TopPrecipitation.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1); // single reducer to get max

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
