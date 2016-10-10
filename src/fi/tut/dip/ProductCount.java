
package fi.tut.dip;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;



public class ProductCount {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text>{

        private Text word = new Text();

        private String extract_ip(String[] line) {
            return line[0];
        }

        private String extract_product(String[] line) {
            String[] products = line[6].replace("%20", " ").split("/");
            String result = "";
            for (String p: products) {
                if (!p.equals(p.toLowerCase()) & p.length() > 0) {
                    result = p;
                    break;
                }
            }
            return result;
        }

        private String extract_datetime(String[] line) {
            return line[3].split("\\[")[1];
        }

        private String extract_requesttype(String[] line) {
            String[] words = line[6].replace("%20", " ").split("/");
            if (Arrays.asList(words).contains("add_to_cart")) return "addtocart";
            else if (Arrays.asList(words).contains("checkout")) return "checkout";
            else if (Arrays.asList(words).contains("view_cart")) return "viewcart";
            else if (Arrays.asList(words).contains("contact_us")) return "contact";
            else return "browsing";
        }

        protected void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            String type = extract_requesttype(words);
            String ip = extract_ip(words);
            String product = extract_product(words);
            String time = extract_datetime(words);
            context.write(new Text(time), new Text(ip + "_" + type + "_" + product + "\n"));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Map<String, Integer> products_by_browses = new HashMap<String, Integer>();
        private Map<String, Integer> hours_by_browses = new HashMap<String, Integer>();
        private MultipleOutputs mos;

        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        private void increment_record(Map<String, Integer> a_map, ArrayList<String> a_record) {
            for (String product : a_record) {
                if (!a_map.containsKey(product)) a_map.put(product, 1);
                else a_map.put(product, a_map.get(product) + 1);
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {
            for (Text value: values) {
                String[] components = value.toString().split("\n")[0].split("_");
                String rq = components[1];
                String product_name = "";
                String time = key.toString();

                // most browsed products
                if (components.length > 2) product_name = components[2];
                if (rq.equals("browsing")) {
                    if (!product_name.equals("")) {
                        ArrayList<String> a_array = new ArrayList<String>();
                        a_array.add(product_name);
                        increment_record(products_by_browses, a_array);
                    }
                }

                // busiest hours
                String hr = time.split("/")[2].split(":")[1];
                if (!hours_by_browses.containsKey(hr)) hours_by_browses.put(hr, 1);
                else hours_by_browses.put(hr, hours_by_browses.get(hr)+1);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (String name: products_by_browses.keySet()) {
                mos.write("browses", new Text(name), new Text(products_by_browses.get(name).toString()));
            }
            for (String hr: hours_by_browses.keySet()) {
                mos.write("hours", new Text(hr), new Text(hours_by_browses.get(hr).toString()));
            }
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Product Count");

        job.setJarByClass(ProductCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        MultipleOutputs.addNamedOutput(job, "browses", TextOutputFormat.class, LongWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "hours", TextOutputFormat.class, LongWritable.class, Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            TextInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
