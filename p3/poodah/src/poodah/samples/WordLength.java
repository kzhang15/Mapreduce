package poodah.samples;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import poodah.MapReduce.Mapper;
import poodah.MapReduce.Reducer;
import poodah.conf.JobConfig;
import poodah.io.keyvals.IntTextOutputKeyVal;
import poodah.io.utils.OutputCollector;
import poodah.io.utils.formats.IntTextOutputFormat;
import poodah.io.utils.formats.TextInputFormat;
import poodah.startup.JobClient;


public class WordLength {


	public static class WordLengthMap implements Mapper<Integer, String, Integer, String> {

		public void map(Integer key, String value,
				OutputCollector<Integer, String> collector) throws IOException {
			if (value.trim().length() != 0) {
				String[] tokens = value.split(" ");
				for (String s: tokens) {
					String k = s.trim().replaceAll("[\\[\\]()?\":!.,;']+", "");
					k = k.toLowerCase();
					int length = k.length();
					try {
						if (k.length() != 0)
							collector.collect(length, k);
					} catch (InstantiationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			return;
		}	
	}


	public static class WordLengthReduce implements Reducer<Integer, String, Integer, String> {
		public void reduce(Integer key, List<String> values,
				OutputCollector<Integer,String> collector) throws IOException {
			String result = "";
			for (String s: values) {
				result += s;
				result +=" ";
			}
			try {
				collector.collect(key, result);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
	}
	public static class MyComparator implements Comparator<Integer> {

		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}

	}



	public static void main(String[] args) throws IOException, 
	IllegalArgumentException, ClassNotFoundException, InstantiationException,
	IllegalAccessException {

		JobConfig conf = new JobConfig();
		conf.setMapCompare(MyComparator.class);
		conf.setReduceCompare(MyComparator.class);
		conf.setMapper(WordLengthMap.class);
		conf.setReducer(WordLengthReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(IntTextOutputFormat.class);
		conf.setMapOutputKeyVal(IntTextOutputKeyVal.class);
		conf.setReduceOutputKeyVal(IntTextOutputKeyVal.class);
		conf.setRecordByteLength(2048);
		conf.setInputFile("/afs/andrew.cmu.edu/usr/keweiz/private/15-440/p3/fileTests/raplyrics.txt");
		conf.setNumKeysReducer(2);
		conf.setReaderSize(2);
		conf.setOutputFile("/afs/andrew.cmu.edu/usr/keweiz/private/15-440/p3/fileTests/raplyrics_out.txt");
		conf.setMasterServer("ghc26.ghc.andrew.cmu.edu");
		System.out.println("JobId: " + JobClient.runJob(conf));

	}
}
