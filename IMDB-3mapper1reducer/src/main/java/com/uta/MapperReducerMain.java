package com.uta;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapperReducerMain {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private final static Text outValue = new Text();

		// Mapper for processing title_basics data
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split("\t");
				String titleId = result[0];
				String titleType = result[1];
				String primaryTitle = result[2];
				String startYear = result[5];
				String genre = result[8];

				if (!titleId.equals("\\N") && !startYear.equals("\\N") && titleType.equals("movie")) {
					int startyear = Integer.parseInt(startYear);
					if (startyear >= 1950 && startyear <= 1960) {
						String midKey = titleType + "\t" + genre + "\t" + primaryTitle + "\t" + "startYear."
								+ startYear;
						word.set(titleId);
						outValue.set(midKey);
						context.write(word, outValue);
					}
				}
			}
		}
	}

	// Mapper for processing title-actors data
	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private final static Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split(",");

				String titleId = result[0];
				String actorId = result[1];
				String actorName = result[2];

				if (!titleId.equals("\\N") && !actorId.equals("\\N")) {
					String midKey = "actor." + actorId + "\t" + "actorName." + actorName;
					word.set(titleId);
					outValue.set(midKey);
					context.write(word, outValue);
				}
			}
		}
	}

	// Mapper for processing title-crew data
	public static class TokenizerMapper3 extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private final static Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				String[] result = str.toString().split("\t");

				String titleId = result[0];
				String directors = result[1];

				if (!titleId.equals("\\N") && !directors.equals("\\N")) {
					word.set(titleId);
					outValue.set("director." + directors);
					context.write(word, outValue);
				}
			}
		}
	}

	/*
	 * merge the output from all the mappers and emits the records which have same
	 * actor-id and director-id
	 */
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String res = "";
			for (Text val : values) {
				res = res + "\t" + val;
			}
			String[] reschk = res.toString().split("\t");
			if (res.contains("actor.") && res.contains("director.") && res.contains("startYear.")) {
				String[] actorid = new String[10];
				String[] actorName = new String[10];
				String[] directorid = new String[10];
				int n = reschk.length;
				int j = 0, l = 0;
				for (int i = 0; i < n; i++) {
					if (reschk[i].contains("actor.")) {
						actorid[j] = reschk[i].substring(6);
						if (i + 1 < n) {
							actorName[j] = reschk[i + 1];
						}
						j++;
					}
					if (reschk[i].contains("director.")) {
						directorid[l] = reschk[i];
						l++;
					}
				}
				String[] commonactorid = new String[j];
				String[] commonactorName = new String[j];
				String[] commondirectorid = new String[j];
				int i = 0;
				for (int k = 0; k < j; k++) {
					for (int m = 0; m < l; m++) {
						if (directorid[m].contains(actorid[k])) {
							commonactorid[i] = actorid[k];
							commonactorName[i] = actorName[k];
							commondirectorid[i] = directorid[m];
							i++;
						}
					}
				}
				for (int k = 0; k < j; k++) {
					res = res.replace("actor." + actorid[k] + "\t", "");
					res = res.replace("actor." + actorid[k], "");
					res = res.replace(actorName[k] + "\t", "");
					res = res.replace(actorName[k], "");

				}

				for (int m = 0; m < l; m++) {
					res = res.replace(directorid[m] + "\t", "");
					res = res.replace(directorid[m], "");
				}
				for (j = 0; j < i; j++) {
					String restmp = res;
					restmp = restmp.concat("\t" + "actor." + commonactorid[j]);
					restmp = restmp.concat("\t" + commonactorName[j]);
					restmp = restmp.concat("\t" + commondirectorid[j]);
					result.set(restmp);
					context.write(key, result);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		long timeStart = System.currentTimeMillis();
		System.out.println(timeStart);

		Configuration conf = new Configuration();
		int split = 730 * 1024 * 1024; // Size of large file in bytes
		String splitsize = Integer.toString(split);
		conf.set("mapreduce.input.fileinputformat.split.minsize", splitsize);
		Job job = Job.getInstance(conf, "Imdb 3 mapper 1 Reducer");
		job.setJarByClass(MapperReducerMain.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TokenizerMapper3.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.waitForCompletion(true);

		long timeEnd = System.currentTimeMillis();
		double diff = (timeEnd - timeStart);
		System.out.println(timeEnd);
		System.out.println("Time taken by job for completion in millis: " + diff);
	}
}
