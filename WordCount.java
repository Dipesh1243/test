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
	

  public static class WCMapper extends Mapper<Object, Text, Text, Text>{
  
		private Text sortedText = new Text();
        private Text originalText = new Text();
        
    public void map(Object key, Text value, Context context) 
    
    throws IOException, InterruptedException {
    
     String word = value.toString();
     char[] arr = word.toCharArray();
     Arrays.sort(arr);
     String wordKey = new String(arr);
     sortedText.set(wordKey);
     originalText.set(word);
     context.write(sortedText, originalText);
      
    }
  }

  public static class WCReducer extends Reducer<Text,Text,Text,Text> {
              
              private Text outputValue = new Text();
        	  private Text outputKey = new Text();
              
              
    public void reduce(Text key, Iterable<Text> values, Context context)
    
     throws IOException, InterruptedException {
     
      String output = "";
	for(Text i : values) {
     
      if(!output.equals(""))
      {
      output = output + "~";
      }
     
     output = output + i.toString(); 
     }
     StringTokenizer outputTokenizer = new StringTokenizer(output,"~");
     
     if(outputTokenizer.countTokens()>=2)
      {
                   output = output.replace("~", ",");
                   outputKey.set(key.toString());
                   outputValue.set(output);
				context.write(outputKey, outputValue);
     }
      
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