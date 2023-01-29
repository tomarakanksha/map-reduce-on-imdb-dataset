package com.uta;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperReducerMain2temp {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private final static Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split("\t");
				// System.out.println(result[0]+" +"+result[1]+ " +"+ result[2]+ "+ "+ result[3]
				// );
				// int n = result.length;

				String titleId = result[0];
				String titleType = result[1];
				String primaryTitle = result[2];
				String startYear = result[5];
				String endYear = result[6];

				if (!titleId.equals("\\N") && !startYear.equals("\\N") && titleType.equals("movie")) {
					int startyear=Integer.parseInt(startYear);
					if(startyear>=1950 && startyear<=1960) {	//filter for STARTYEAR in range(1950 and 1960)
					String midKey = titleType + "\t" + primaryTitle + "\t" + "startYear."+startYear;
					word.set(titleId);
					outValue.set(midKey);
					context.write(word, outValue);
					}
				}
			}
		}
	}

	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, Text> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private final static Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// FileSplit fileSplit = (FileSplit) context.getInputSplit();
			// String filename = fileSplit.getPath().getName();

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split(",");
				// System.out.println(result[0]+" +"+result[1]+ " +"+ result[2] );
				// int n = result.length;

				String titleId = result[0];
				String actorId = result[1];
				String actorName = result[2];

				if (!titleId.equals("\\N") && !actorId.equals("\\N") &&
				!actorName.equals("\\N")) {
					String midKey = "actor."+actorId + "\t" + actorName;
					word.set(titleId);
					outValue.set(midKey);
					context.write(word, outValue);
				}
			}
		}
	}

	public static class TokenizerMapper3 extends Mapper<Object, Text, Text, Text> {

		// private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private final static Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// FileSplit fileSplit = (FileSplit) context.getInputSplit();
			// String filename = fileSplit.getPath().getName();

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split("\t");
				// System.out.println(result[0]+" +"+result[1] );
				// int n = result.length;

				String titleId = result[0];
				String directors = result[1];
				
				if (!titleId.equals("\\N") && !directors.equals("\\N")) {
				word.set(titleId);
				outValue.set("director."+directors);
				context.write(word, outValue);
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String res = "";
			for (Text val : values) {
				res = res + "\t" + val;
			}
			String[] reschk = res.toString().split("\t");
			//System.out.println(res);
			//System.out.println(reschk.length);
			if (res.contains("actor.")&& res.contains("director.") && res.contains("startYear.")) {
				//System.out.println(res);
				result.set(res);
				context.write(key, result);
			}
		}
	}
	
	public static class TokenizerMapperMap2 extends Mapper<Object, Text, Text, Text> {

		private final static Text val = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split("\t");
				// System.out.println(result[0]+" +"+result[1] );
				int n = result.length;
				String titleId = result[0];
				String val1 = "";
				for (int i=1;i<n;i++) {
					val1=val1+"\t"+result[i];
				}
				//System.out.println(val1);
				// if (!titleId.equals("\\N") && !actorId.equals("\\N") &&
				// !actorName.equals("\\N")) {
				word.set(titleId);
				val.set(val1);
				context.write(word, val);
			}
		}
	}
	
	public static class IntSumReducerRed2 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String res = "";
			for (Text val : values) {
				res = res + "\t" + val;
			}
			String[] reschk = res.toString().split("\t");
			String[] actorid= {"","","","","","","","","",""};
			String directorid="";
			int startYear=0;
			int n= reschk.length;
			//System.out.println(res);
			int j=0;
			for (int i=0;i<n;i++) {
				if(reschk[i].contains("actor.")){
					actorid[j]=reschk[i].substring(2);
					j++;
					//System.out.println("actorid:"+actorid );
				}
				if(reschk[i].contains("director.")){
					directorid=reschk[i];
					//System.out.println("directorid:"+directorid );
				}
				if(reschk[i].contains("startYear"))
				{
					if(!reschk[i].contains("\\N")) {
					startYear=Integer.parseInt(reschk[i].substring(10));
					//System.out.println("startYear:"+startYear);
					}
				}
			}
			for(int k =0; k<j;k++) {
				if(directorid.contains(actorid[k])){
					result.set(res);
					context.write(key, result);
				}
			}
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println(Time.now());
		String Pathtmp= "/inputMapReduceTmp";
		System.out.println("Hello");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WORDCOUNT 1");
		job.setJarByClass(MapperReducerMain2temp.class);
		// job.setMapperClass(TokenizerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TokenizerMapper3.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job, new Path(args[3]));
		//FileOutputFormat.setOutputPath(job, new Path(Pathtmp+"1"));
		FileOutputFormat.setOutputPath(job, new Path(Pathtmp));
		//FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.waitForCompletion(true);
		//System.out.println("titleid:"+ job.getInstance() );

		/////////////// chain
		Job job2 = Job.getInstance(conf, "word count second part");
		job2.setJarByClass(MapperReducerMain2temp.class);
		// job.setMapperClass(TokenizerMapper.class);
		MultipleInputs.addInputPath(job2, new Path(Pathtmp), TextInputFormat.class, TokenizerMapperMap2.class);
		// job.setCombinerClass(IntSumReducer.class);
		job2.setReducerClass(IntSumReducerRed2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		//job2.waitForCompletion(true);
		System.out.println("2nd map reduce started" );
		System.out.println(Time.now());
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
/*
 * SELECT count( *) FROM    imdb00.title_basics b 
INNER JOIN imdb00.TITLE_PRINCIPALS a ON  a.TCONST=b.TCONST  and a.characters<> '\N' and a.category in ('actor','actress') and a.NCONST<>'\N'
INNER join imdb00.title_crew c ON a.TCONST=c.TCONST and c.directors<> '\N' 
and c.directors contains( a.NCONST )
Where b.Titletype='movie' AND b.startYear BETWEEN '1950' AND '1960'
ORDER BY a.TCONST DESC;
 */
}

