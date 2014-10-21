package org.hwone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.*;

public class QueryAuthor {


  public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

                String author;
                String book;
                String line = value.toString();
                String[] tuple = line.split("\\n");
                try{
                        for(int i=0;i<tuple.length; i++){
                                JSONObject obj = new JSONObject(tuple[i]);
                                author = obj.getString("author");
                                book = obj.getString("book");
                                context.write(new Text(author), new Text(book));
                        }
                }catch(JSONException e){
                        e.printStackTrace();
                }
        }
  }

  public static class Combine extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

                String booklist = null;
                for(Text val : values){
                        if(booklist != null){
                                booklist = booklist + ":::" + val.toString();
                        }
                        else{
                                booklist = val.toString();
                        }
                }
                context.write(key, new Text(booklist));
        }
  }

  public static class Reduce extends Reducer<Text,Text,JSONObject,NullWritable>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		Configuration conf = context.getConfiguration();
                String input = conf.get("inputauthor");
		String author = null;
                try{
                        JSONArray ja = new JSONArray();
                        String[] book = null;
                        for(Text val : values){
                                book = val.toString().split(":::");
                        }
                        for(int i=0; i<book.length; i++){
                                JSONObject jo = new JSONObject().put("book", book[i]);
                                ja.put(jo); 
                        }
                        JSONObject obj = new JSONObject();
                        obj.put("books", ja);
                        obj.put("author", key.toString());
			author = obj.getString("author");
			if(input.equals(author)){
				context.write(obj, NullWritable.get());
			}
                }catch(JSONException e){
                        e.printStackTrace();
                }
        }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

    if (otherArgs.length < 3) {
      System.err.println("Usage: QueryAuthor <in> <out> <author>");
      System.exit(2);
    }

    String input = null;
    for(int i=2;i<otherArgs.length;i++){
	if(input != null){
		input = input + " " + otherArgs[i];
	}else{
		input = otherArgs[i];
	}
    }
    conf.set("inputauthor", input);
    Job job = new Job(conf, "QueryAuthor");
    job.setJarByClass(QueryAuthor.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Combine.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(JSONObject.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

