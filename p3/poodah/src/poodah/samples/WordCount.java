package poodah.samples;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import poodah.MapReduce.Mapper;
import poodah.MapReduce.Reducer;
import poodah.conf.JobConfig;
import poodah.io.keyvals.TextIntOutputKeyVal;
import poodah.io.utils.OutputCollector;
import poodah.io.utils.formats.TextInputFormat;
import poodah.io.utils.formats.TextIntOutputFormat;
import poodah.startup.JobClient;

public class WordCount {

	public static class WordCountMap implements Mapper<Integer, String, String, Integer> {

		@Override
		public void map(Integer key, String value,
				OutputCollector<String, Integer> collector) throws IOException {
			if (value.trim().length() != 0) {
				String[] tokens = value.split(" ");
				for (String s: tokens) {
					if (s.trim().length() > 0) {				
						String k = s.replaceAll("[\\[\\]\"()?:!.,;']+", "");
						k = k.toLowerCase().trim();
						try {
							if(k.length() !=0)
								collector.collect(k, 1);
						} catch (InstantiationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
					}
				}
			}
			return;
		}		
	}

	public static class WordCountReduce implements Reducer<String, Integer, String, Integer> {
	
		@Override
		public void reduce(String key, List<Integer> values,
				OutputCollector<String,Integer> collector) {
			int sum = 0;
			for (Integer i: values) {
				sum += i;
			}
			try {
				collector.collect(key,sum);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static class MyComparator implements Comparator<String> {

		@Override
		public int compare(String o1, String o2) {
			return o1.compareTo(o2);
		}

	}
	public static void main(String[] args) {
		JobConfig conf = new JobConfig();
		conf.setMapCompare(MyComparator.class);
		conf.setReduceCompare(MyComparator.class);
		conf.setMapper(WordCountMap.class);
		conf.setReducer(WordCountReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextIntOutputFormat.class);
		conf.setMapOutputKeyVal(TextIntOutputKeyVal.class);
		conf.setReduceOutputKeyVal(TextIntOutputKeyVal.class);
		conf.setRecordByteLength(2048);
		conf.setInputFile("/afs/andrew.cmu.edu/usr/keweiz/private/15-440/p3/fileTests/analects.txt");
		conf.setNumKeysReducer(2);
		conf.setReaderSize(2);
		conf.setOutputFile("/afs/andrew.cmu.edu/usr/keweiz/private/15-440/p3/fileTests/analects_out.txt");
		conf.setMasterServer("ghc26.ghc.andrew.cmu.edu");
		System.out.println("JobId: " + JobClient.runJob(conf));
	}

}
