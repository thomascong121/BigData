package assignment1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * This class solves the problem posed for Assignment1
 *
 */
public class Assignment1 {
	public static class MyMapper extends Mapper<Object, Text,Text,Text>{
		private Text word = new Text();
		private Text occur = new Text();
		@Override
		public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
			//Take the file names
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			//split up the file into tokens
			String line = value.toString();
			String[] splited = line.split(" ");
			//obtain first input argument
			int n = Integer.parseInt(context.getConfiguration().get("ngram"));
			//implement method to obtain ngrams
			for (int i = 0; i <= splited.length - n + 1; i ++) {
				List<String> temp = new ArrayList<String>();
				if((n + i-1) < splited.length){
					for (int j = i;j < (n + i);j += 1){
						temp.add(splited[j]);
					}	
					//set up map function output as
					//(ngarm, filename)
					word.set(String.join(" ", temp));
					occur.set(filename);
					context.write(word,occur); 
				}
			} 
		}
 }
 
 public static class MyReducer extends Reducer<Text, Text, Text, Text>{
	 private Text result = new Text();
	 @Override 
	 protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		 int count = 0;
		 //obtain second input argument
         int occur = Integer.parseInt(context.getConfiguration().get("occur"));
         List<String> temp = new ArrayList<String>();
         String cc = "";
         String files = "";
         //count the number of files as occurence of the ngram
         while (values.iterator().hasNext()) {
        	 cc = values.iterator().next().toString();
             if(!temp.contains(cc)){
            	 temp.add(cc);
             }
             count++;
         }   
         //sort the file names
         Collections.sort(temp);
         files = String.join(" ", temp);
         //if the occurence is larger than the threshold
         if(count >= occur){
        	 result.set(Integer.toString(count) + "  " + files);
             context.write(key, result);
         }
	 }
 }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //Take arguments
        conf.set("ngram", args[0]);  
        conf.set("occur", args[1]); 
        Job job = Job.getInstance(conf, Assignment1.class.getSimpleName()); 
        //Output as jar
        job.setJarByClass(Assignment1.class);
        //location of the input file
        FileInputFormat.addInputPaths(job, args[2]);
        //set mapper
        job.setMapperClass(MyMapper.class);
        //output data type of mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //set reducer
        job.setReducerClass(MyReducer.class);
        //output data type of reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //location of the output file
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}