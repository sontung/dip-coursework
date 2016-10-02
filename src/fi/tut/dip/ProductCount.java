
package fi.tut.dip;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.*;
import java.io.*;
import java.nio.charset.Charset;



public class ProductCount {
    private ArrayList<String> CONTENT;

    public ProductCount() {
        CONTENT = new ArrayList<String>();
        BufferedReader br = null;

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("/home/hduser/Desktop/log-data/access.log.2"));

            while ((sCurrentLine = br.readLine()) != null) {
                CONTENT.add(sCurrentLine);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void print_content() {
        System.out.println(CONTENT.size());
        System.out.print(CONTENT);
    }

    public String extract_ip(String line) {
        return line.split(" ")[0];
    }

    public String extract_product(String line) {
        String[] products = line.split(" ")[6].replace("%20", " ").split("/");
        String result = "";
        for (String p: products) {
            if (!p.equals(p.toLowerCase()) & p.length() > 0) {
                result = p;
                break;
            }
        }
        return result;
    }

    public String request_type(String line) {
        String[] words = line.split(" ")[6].replace("%20", " ").split("/");
        if (Arrays.asList(words).contains("add_to_cart")) return "addtocart";
        else if (Arrays.asList(words).contains("checkout")) return "checkout";
        else if (Arrays.asList(words).contains("view_cart")) return "viewcart";
        else if (Arrays.asList(words).contains("contact_us")) return "contact";
        else return "browsing";
    }

    public void increment_record(Map<String, Integer> a_map, ArrayList<String> a_record) {
        for (String product: a_record) {
            if (!a_map.containsKey(product)) a_map.put(product, 1);
            else a_map.put(product, a_map.get(product)+1);
        }
    }

    public void print_map(Map<String, Integer> a_map) {

    }

    public Map<String, Integer> best_selling() {
        Map<String, Integer> all_products = new HashMap<String, Integer>();
        Map<String, ArrayList<String>> products_by_ip = new HashMap<String, ArrayList<String>>();
        for (int i = 0; i < CONTENT.size(); i++) {
            String request = CONTENT.get(i);
            String ip = extract_ip(request);
            if (request_type(request).equals("addtocart")) {
                if (!products_by_ip.containsKey(ip)) {
                    ArrayList<String> a_array = new ArrayList<String>();
                    a_array.add(extract_product(request));
                    products_by_ip.put(ip, a_array);
                } else {
                    products_by_ip.get(ip).add(extract_product(request));
                }
            } else if (request_type(request).equals("checkout")) {
                if (products_by_ip.containsKey(ip)) {
                    System.out.println(products_by_ip.get(ip));
                    increment_record(all_products, products_by_ip.get(ip));
                    products_by_ip.put(ip, new ArrayList<String>());
                }
            }
        }
        return all_products;
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private static final String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        /*Your Mapper Code here*/


    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        /*Your Reducer Code here*/

    }

    public static void main(String[] args) throws Exception {
//
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }
//
//        Job job = Job.getInstance(conf, "Product Count");
//
//        job.setJarByClass(ProductCount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        for (int i = 0; i < otherArgs.length - 1; ++i) {
//            TextInputFormat.addInputPath(job, new Path(otherArgs[i]));
//        }
//
//        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        ProductCount pc = new ProductCount();
        //pc.print_content();
        System.out.println(pc.best_selling());
    }
}
