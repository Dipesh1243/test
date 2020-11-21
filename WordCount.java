import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.HashSet;
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
	
  public static class WCMapper extends Mapper<Object, Text, Text, Text>{
  
  static Collection<Text> anagrams = new HashSet<Text>();

	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
      StringTokenizer token = new StringTokenizer(value.toString());

      while (token.hasMoreTokens()) {
	  String word = token.nextToken().replaceAll("\\W", "");
                char[] array = word.toCharArray();
                Arrays.sort(array);
                String key = new String(array);
                context.write(new Text(key), new Text(key));
      }
    }
  }

  public static class WCReducer extends Reducer<Text,Text,Text,Text> {
              
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
     
        Collection<Text> anagrams = new HashSet<Text>();

            String anagram = null;

            for (Text val : values) {

                if (anagram == null) {

                    anagram = val.toString();

                    

                } else {

                    anagram = anagram + ',' + val.toString();

                }

                 anagrams.add(val);

            }

            context.write(key, new Text(anagram));

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
