import java.io.IOException;
import java.util.*;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.taglibs.standard.tag.common.core.ParamSupport;

public class TopTen {

    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
                    .split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String userId = " ";
            String reputation = " ";
            //Get the string xml line by line
            Scanner scanner = new Scanner( new File("/Users/liu/Github/DIC_lab/mywork/lab1/ten/input/users.xml"), "UTF-8" );

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                Map<String, String> parsed = transformXmlToMap(line);
                userId = parsed.get("Id");
                reputation = parsed.get("Reputation");
//                System.out.print(parsed.get("Id")+"  "+parsed.get("Reputation")+"\n");
                // check that this row contains user data
                if(userId == null) {
                }
                else{
                    //Add this record to our map  with the reputation as the key
                    repToRecordMap.put(Integer.parseInt(reputation),new Text(userId));
                    // If we have more than ten records, remove the one with the lowest reputation.
                    if (repToRecordMap.size() > 10) {
                        // firstKey(), Returns the first (lowest) key currently in this map.
                        repToRecordMap.remove(repToRecordMap.firstKey());
                    }
                }
            }
            System.out.print(repToRecordMap);
            scanner.close();
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        // Overloads the comparator to order the reputations in descending order
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String userId = " ";
            String reputation = " ";
            //Get the string xml line by line
            Scanner scanner = new Scanner( new File("/Users/liu/Github/DIC_lab/mywork/lab1/ten/input/users.xml"), "UTF-8" );

            //values The value from those top 10 reputation
            for (Text value : values) {

                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        Map<String, String> parsed = transformXmlToMap(line);
                        userId = parsed.get("Id");
                        reputation = parsed.get("Reputation");
    //                System.out.print(parsed.get("Id")+"  "+parsed.get("Reputation")+"\n");
                        // check that this row contains user data
                        if(userId == null) {
                        }
                        else{
                            if(value.toString() == userId)
                                System.out.print(line);
                            //Add this record to our map with the reputation as the key
                            repToRecordMap.put(Integer.parseInt(reputation),new Text(userId));
                            // If we have more than ten records, remove the one with the lowest reputation.
                            if (repToRecordMap.size() > 10) {
                                // firstKey(), Returns the first (lowest) key currently in this map.
                                repToRecordMap.remove(repToRecordMap.firstKey());
                            }
                        }
                    }
                }
                // Sort in descending order
                for (Text t : repToRecordMap.descendingMap().values()) {
                    // Output our ten records to the file system with a null key
                    context.write(NullWritable.get(), t);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top ten");
//      Set only one reducer
        job.setNumReduceTasks(1);
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TopTenMapper.class);
        job.setCombinerClass(TopTenReducer.class);
        job.setReducerClass(TopTenReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
//      Configure the input/output path from the file system into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


