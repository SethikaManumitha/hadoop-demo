package org.sethika;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

public class DistrictPerMonth {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
        private HashMap<String, String> locationMap = new HashMap<>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void configure(JobConf job) {
            // Load the cached location data into the HashMap - Map Side join
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    // Process the cached files as needed
                    BufferedReader reader = new BufferedReader(new FileReader("locationData.csv"));
                    String line;
                    reader.readLine();
                    while ((line = reader.readLine()) != null) {
                        String[] columns = line.split(",");
                        String locationId = columns[0];
                        String city = columns[7]; // City Name
                        locationMap.put(locationId, city);
                    }
                    reader.close();
                }
            } catch (IOException e) {
                System.out.println("Error reading location file: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            // Skip header
            if(line.startsWith("location_id")){
                return;
            }

            String[] columns = line.split(",");
            String locationId = columns[0];
            String date = columns[1];
            double temp = Double.parseDouble(columns[5]); // temperature_2m_mean
            int precipitation = Integer.parseInt(columns[12]); // precipitation_hours

            String city = locationMap.getOrDefault(locationId,"Unknown");

            String[] dateParts = date.split("-");
            String year = dateParts[0];
            String month = dateParts[1];

            outputKey.set(city + "_" + year + "_" + month);
            outputValue.set(temp + "," + precipitation);

            output.collect(outputKey, outputValue);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double totalTemp = 0.0;
            int totalHoursPrecipitation = 0;
            int count = 0;

            while(values.hasNext()){
                String[] valueParts = values.next().toString().split(",");
                totalTemp += Double.parseDouble(valueParts[0]);
                totalHoursPrecipitation += Integer.parseInt(valueParts[1]);
                count++;
            }
            double avgTemp = totalTemp / count;
            output.collect(key, new Text(avgTemp + "," + totalHoursPrecipitation));
        }
    }
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(DistrictPerMonth.class);
        conf.setJobName("DistrictPerMonthWeather");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        DistributedCache.addCacheFile(new URI("hdfs://namenode:9000/data/processed_location_data.csv#locationData.csv"), conf);

        JobClient.runJob(conf);
    }
}
