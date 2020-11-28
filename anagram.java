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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Anagram {

    public static String[] stopwords = {
            "'tis", "'twas", "a", "able", "about", "across", "after", "ain't", "all", "almost", "also", "am",
            "among", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "but", "by", "can", "can't", "cannot",
            "could", "could've", "couldn't", "dear", "did", "didn't", "do", "does", "doesn't", "don't", "either", "else", "ever", "every",
            "for", "from", "get", "got", "had", "has", "hasn't", "have", "he", "he'd", "he'll", "he's", "her", "hers", "him", "his", "how", "how'd",
            "how'll", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "just", "least",
            "let", "like", "likely", "may", "me", "might", "might've", "mightn't", "most", "must", "must've", "mustn't", "my", "neither",
            "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "shan't",
            "she", "she'd", "she'll", "she's", "should", "should've", "shouldn't", "since", "so", "some", "than", "that", "that'll", "that's",
            "the", "their", "them", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "tis", "to",
            "too", "twas", "us", "wants", "was", "wasn't", "we", "we'd", "we'll", "we're", "were", "weren't", "what", "what'd", "what's", "when",
            "when'd", "when'll", "when's", "where", "where'd", "where'll", "where's", "which", "while", "who", "who'd", "who'll", "who's",
            "whom", "why", "why'd", "why'll", "why's", "will", "with", "won't", "would", "would've", "wouldn't", "yet", "you", "you'd", "you'll", "you're", "you've", "your"};


	/*The main function of the Mapper is to read data from an input file string by string and create
	 a token which can be used to form a "key" in order to group the anagrams, this "key" is passed onto the reducer*/

    public static class AnagramMapper extends Mapper<Object, Text, Text, Text> {

        private Text keyword = new Text();
        private Text anagramword = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //string tokenizer allows us to convert a string into a token, the "value.toString" will return an integer value that represents the string
            StringTokenizer token = new StringTokenizer(value.toString());
			/*the while loop makes use of the hasMoreTokens method and will continue to run if there are more tokens available,
			it is the algorithm used to pull out the key and words from the input file*/
            while (token.hasMoreTokens()) {
            /*A string "word" is set to return the next token from the StringTokenizer,
             however all special characters in that string will be removed and the string will be changed to lowercase*/
                String word = token.nextToken().replaceAll("[^a-z A-Z]", "").toLowerCase(); //FIX SO NO DAM NUMBERS COMES
                char[] achar = word.toCharArray();
                Arrays.sort(achar);
                String wordKey = new String(achar).toLowerCase();
                keyword.set(wordKey);
                anagramword.set(word);
                context.write(keyword, anagramword);
            }
        }
    }

    public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {

        private Text sortedtext = new Text();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> sorted = new HashMap<String, Integer>();
            LinkedHashMap<String, Integer> sorted1 = new LinkedHashMap<String, Integer>();


            for (Text val : values) {


                if (sorted.containsKey(val.toString())) {

                    sorted.put(val.toString(), sorted.get(val.toString()) + 1);

                } else {

                    sorted.put(val.toString(), 1);
                }

            }

            for (int j = 0; j < stopwords.length; j++) {
                if (sorted.containsKey(stopwords[j])) {
                    sorted.remove(stopwords[j]);
                }
            }

           // List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(sorted.entrySet());


@Override
sorted.entrySet()
    .stream()
    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())) 
    .forEachOrdered(x -> sorted1.put(x.getKey(), x.getValue()));
    
    
          /*  Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
               @Override
                public int compare(Map.Entry<String, Integer> x,
                                   Map.Entry<String, Integer> y) {
                    return -1 * (x.getValue()).compareTo(y.getValue());
                }
            });*/


          /*  for (Map.Entry<String, Integer> listval : list) {
                sorted1.put(listval.getKey(), listval.getValue());
            }*/

            if (sorted1.size() > 1) {

                sortedtext.set(sorted1.toString());
                context.write(key, sortedtext);

            }
        }
    }


    public static class AnagramMapperAlphabet extends Mapper<Text, Text, Text, Text> {

        private Text anagram2 = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String newVal = value.toString();
            anagram2.set(newVal);
            context.write(anagram2, key);


        }
    }

    public static class AnagramReducerAlphabet extends Reducer<Text, Text, Text, Text> {
        private Text anagram3 = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                anagram3.set(val);
                context.write(key, anagram3);
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort by frequency");
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMapper.class);
        job.setReducerClass(AnagramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int val = job.waitForCompletion(true) ? 0 : 1;

        if (val == 0) {

            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "sort alphabetically");
            job2.setJarByClass(Anagram.class);
            job2.setMapperClass(AnagramMapperAlphabet.class);
            job2.setReducerClass(AnagramReducerAlphabet.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/secondoutput"));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        }


    }
}
