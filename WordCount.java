import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Collection;
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
	//static Collection<Text> anagrams = new HashSet<Text>();

  public static class WCMapper
       extends Mapper<Object, Text, Text, IntWritable>{
private final static IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) 
    
    throws IOException, InterruptedException {
    
      StringTokenizer itr = new StringTokenizer(value.toString());

      while (itr.hasMoreTokens()) {
	 String word = itr.nextToken();
                char[] arr = word.toCharArray();
                Arrays.sort(arr);
                String wordKey = new String(arr);
                context.write(new Text(wordKey), new IntWritable(word));
      }
    }
  }

  public static class WCReducer
  
       extends Reducer<Text,Text,Text,IntWritable> {
              private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
     throws IOException, InterruptedException {
     
      String anagram = null;
      
 for (IntWritable val : values) {
     if (anagram == null){
      anagram = val.toString();
     } else {
             anagram = anagram + ',' + val.toString();
     }
      }
       context.write(key, new IntWritable(anagram));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WCMapper.class);
    job.setReducerClass(WCReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}