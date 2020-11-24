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

public class Anagram {

    public static String[] stopwords = {

            "'tis","'twas","a","able","about","across","after","ain't","all","almost","also","am",
            "among","an","and","any","are","aren't","as","at","be","because","been","but","by","can","can't","cannot",
            "could","could've","couldn't","dear","did","didn't","do","does","doesn't","don't","either","else","ever","every",
            "for","from","get","got","had","has","hasn't","have","he","he'd","he'll","he's","her","hers","him","his","how","how'd",
            "how'll","how's","however","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","just","least",
            "let","like","likely","may","me","might","might've","mightn't","most","must","must've","mustn't","my","neither",
            "no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","shan't",
            "she","she'd","she'll","she's","should","should've","shouldn't","since","so","some","than","that","that'll","that's",
            "the","their","them","then","there","there's","these","they","they'd","they'll","they're","they've","this","tis","to",
            "too","twas","us","wants","was","wasn't","we","we'd","we'll","we're","were","weren't","what","what'd","what's","when",
            "when'd","when'll","when's","where","where'd","where'll","where's","which","while","who","who'd","who'll","who's",
            "whom","why","why'd","why'll","why's","will","with","won't","would","would've","wouldn't","yet","you","you'd","you'll","you're","you've","your"};

    public static class WCMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer token = new StringTokenizer(value.toString());

            while (token.hasMoreTokens()) {
                String word = token.nextToken().replaceAll("\\W", "");
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

            for (int j = 0; j < stopwords.length; j++){
                if(arraylist.contains(stopwords[j])){
                    arraylist.remove(stopwords[j]);
                }
            }

            Collections.sort(arraylist);
            anagramword.set(arraylist.toString());
         

            if(arraylist.size() > 1){
                context.write(key, anagramword);
            
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagram");
        job.setJarByClass(Anagram.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        
    }
}
