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
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) 
    
    private Text sortedText = new Text();
    private Text orginalText = new Text();
    
    throws IOException, InterruptedException {
    	
    	 String word = value.toString();
    	 char[] wordChars = word.toCharArray();
    	  Arrays.sort(wordChars);
		String sortedWord = new String(wordChars);
		sortedText.set(sortedWord);
		orginalText.set(word);
   		context.write( sortedText, orginalText );
   		
      }
    }
  

  public static class WCReducer
  
       extends Reducer<Text,Text,Text,Text> {
              
    public void reduce(Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {
     
      String anagram = null;
      
 for (Text val : values) {
     if (anagram == null){
      anagram = val.toString();
     } else {
             anagram = anagram + ',' + val.toString();
     }
      }
       context.write(key, new Text(anagram));
    
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
