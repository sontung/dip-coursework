
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.*;
import java.util.regex.*;
import java.io.*;
import java.nio.charset.Charset;



public class ProductCount {
    private ArrayList<String> CONTENT;

    private String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";

    public ProductCount() {
        CONTENT = new ArrayList<String>();
        BufferedReader br = null;
        ArrayList<String> tokens = new ArrayList<String>();

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("/home/hduser/Desktop/log-data/access.log.2"));

            while ((sCurrentLine = br.readLine()) != null) {
                CONTENT.add(sCurrentLine);
                Pattern p = Pattern.compile(logEntryPattern);
                Matcher m = p.matcher(sCurrentLine);
                while (m.find()) {
                    String token = m.group(8);
                    tokens.add(token);
                }
                System.out.println(Arrays.toString(tokens.toArray()));
                tokens = new ArrayList<String>();
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

    public String extract_ip(String line) {
        return line.split("\\s")[0];
    }

    public String extract_product(String line) {
        String[] products = line.split("\\s")[6].replace("%20", " ").split("/");
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
        String[] words = line.split("\\s")[6].replace("%20", " ").split("/");
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
                    increment_record(all_products, products_by_ip.get(ip));
                    products_by_ip.put(ip, new ArrayList<String>());
                }
            }
        }
        Collection<Integer> v = all_products.values();
        System.out.println(Collections.max(v, null));
        return all_products;
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private static final String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";
        private final static IntWritable one = new IntWritable(1);
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
            String[] words = value.toString().split("\\s");
            String type = extract_requesttype(words);
            String ip = extract_ip(words);
            String product = extract_product(words);
            String time = extract_datetime(words);
            context.write(new Text(type), new Text(ip + "_" + product + "_" + time));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            TextInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
       /* ProductCount pc = new ProductCount();
        //pc.print_content();
        System.out.println(pc.best_selling())*/;
    }
}
