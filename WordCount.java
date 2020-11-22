import java.io.IOException;
import java.sql.Array;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class WCMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String word = itr.nextToken().replaceAll("\\W", "");
                char[] arr = word.toCharArray();
                Arrays.sort(arr);
                String wordKey = new String(arr);
                context.write(new Text(wordKey), new Text(word));
            }
        }
    }

    public static class WCReducer extends Reducer<Text, Text, Text, Text> {
        private Text anagramword = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> arraylist = new ArrayList<String>();
            for (Text val : values) {

                if (arraylist.contains(val.toString())) {
                    continue;

                } else {
                    arraylist.add(val.toString());
                }

            }
            Collections.sort(arraylist);
            anagramword.set(arraylist.toString());
            
            if(arraylist.size() > 1){
                context.write(key, anagramword);
            }


            /*HashSet<String> anagram = new HashSet<>();


            for (Text val : values) {

                anagram.add(val.toString());

            }
            ArrayList<String> list = new ArrayList<String>(anagram);
            Collections.sort(list);
            anagramword.set(list.toString());

            if (anagram.size() > 1) {
                context.write(key, anagramword);
            }*/
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
