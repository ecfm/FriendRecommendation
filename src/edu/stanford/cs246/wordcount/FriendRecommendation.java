package edu.stanford.cs246.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRecommendation extends Configured implements Tool {
   private static HashMap<String, HashSet<String>> target_user_friends; 
   public static void main(String[] args) throws Exception {
	   target_user_friends = new HashMap<String, HashSet<String>>();
	   
//	   target_user_friends.put("43159", new HashSet<String>());
//	   target_user_friends.put("1000", new HashSet<String>());
//	   target_user_friends.put("44373", new HashSet<String>());
//	   target_user_friends.put("20179", new HashSet<String>());
	   
	   target_user_friends.put("924", new HashSet<String>());
	   target_user_friends.put("8941", new HashSet<String>());
	   target_user_friends.put("8942", new HashSet<String>());
	   target_user_friends.put("9019", new HashSet<String>());
	   target_user_friends.put("9020", new HashSet<String>());
	   target_user_friends.put("9021", new HashSet<String>());
	   target_user_friends.put("9022", new HashSet<String>());
	   target_user_friends.put("9990", new HashSet<String>());
	   target_user_friends.put("9992", new HashSet<String>());
	   target_user_friends.put("9993", new HashSet<String>());
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new FriendRecommendation(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "FriendRecommendation");
      job.setJarByClass(FriendRecommendation.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private Text usr_txt = new Text();
      private Text frd_txt = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
      String[] user_friends = value.toString().split("\\t");
      String user = user_friends[0];
      
      // placeholder in case of a target user does not have any friendship recommendation
      if (target_user_friends.containsKey(user)){
    	  usr_txt.set(user);
    	  frd_txt.set("");
    	  context.write(usr_txt, frd_txt);
      }
      if (user_friends.length > 1) { 
	      String[] friends_list = user_friends[1].split(",");
	         for (String friend: friends_list) {
	        	 // populate the corresponding set of direct friends if the 
	        	 // current user is a target for friendship recommendation
		         if (target_user_friends.containsKey(user)){
		            target_user_friends.get(user).add(friend);
		         }
		         
		         // generate a recommended friend candidate for every target 
		         // of friendship recommendation in the current user's friends list
		         if (target_user_friends.containsKey(friend))
		        	 for (String recomm_frd: friends_list){
		        		 if (!recomm_frd.equals(friend)){
			        		 usr_txt.set(friend);
			        		 frd_txt.set(recomm_frd);
			        		 context.write(usr_txt, frd_txt);
		        		 }
		        	 }
	         }
      }
         }
      }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text usr_txt, Iterable<Text> recomm_list, Context context)
              throws IOException, InterruptedException {
         HashMap<String, Integer> candidates = new HashMap<String, Integer>();
 		HashSet<String> direct_friends = target_user_friends.get(usr_txt.toString());
         for (Text recomm_txt: recomm_list) {
        	 String recomm = recomm_txt.toString();
            if (!direct_friends.contains(recomm) && !recomm.equals("")){
            	if (candidates.containsKey(recomm)){
            		Integer incremented_count = candidates.get(recomm)+1;
            		candidates.put(recomm, incremented_count);
            	}
            	else{
            		candidates.put(recomm, 1);
            	}
            }
         }
         Text recommendation = new Text();
         String recomm_results = recommendTopTen(candidates);
         recommendation.set(recomm_results);
         context.write(usr_txt, recommendation);
      }
   }
   
   private static String recommendTopTen(HashMap<String, Integer> candidate_count_map) {
		List<String> list = new ArrayList<String>();
		for (String key : candidate_count_map.keySet()){
			list.add(key+"_"+Integer.toString(candidate_count_map.get(key)));
		}
		Collections.sort(list, new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				String[] kv1 = s1.split("_");
				int k1 = Integer.parseInt(kv1[0]);
				int v1 = Integer.parseInt(kv1[1]);
				String[] kv2 = s2.split("_");
				int k2 = Integer.parseInt(kv2[0]);
				int v2 = Integer.parseInt(kv2[1]);
				if (v1 != v2){
					return Integer.compare(v2, v1);
				}
				else {
					return Integer.compare(k1, k2);
				}
			}
		});
		
		String top10 = "";
		int count = 0;
		for (Iterator<String> it = list.iterator(); it.hasNext();) {
			String entry = it.next();
			String key = entry.split("_")[0];
			if (count > 0){
				top10 += ",";
			}
			top10 += key;
			count += 1;
			if (count == 10){
				break;
			}
			
		}
		return top10;
	}

}
