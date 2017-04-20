
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class Reducely extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	//private Text company_name = new Text();    
	private IntWritable units_sold = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
	      int sum = 0;
	      System.out.println("reducer Input key = " + key.toString() );
		    
	      for(IntWritable val : values) 
		  {
	        sum =sum+ val.get();
	        System.out.println("sum = " + sum );
	      }
	      units_sold.set(sum);
		  context.write(key, units_sold);
	    
	}
	 
}	
	
