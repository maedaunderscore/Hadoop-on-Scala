package hadooptest.newapi

import java.io.IOException
import java.lang.Iterable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.math._

object HadoopConversion{
  implicit def String2Text(value: String) = new Text(value)
  implicit def Text2String(value: Text) = value.toString
  implicit def Int2IntWritable(value: Int) = new IntWritable(value)
  implicit def Writable2Primitive[T](value: Writable{def get():T}) = value.get
  implicit val IntWritableOrdering = new Ordering[IntWritable]{
    def compare(x:IntWritable, y: IntWritable) = x.compareTo(y)
  }
}

class MaxTemperatureMapper extends Mapper[LongWritable, Text, Text, IntWritable]{
  import HadoopConversion._
  override  def map(key:LongWritable, line: Text, 
		    context: Mapper[LongWritable, Text, Text, IntWritable]#Context){
    val year = line.substring(15,19)
    val airTemperature =  if(line.charAt(87) == '+')  line.substring(88,92).toInt
			  else line.substring(87,92).toInt
    val quality = line.substring(92,93);
    if(airTemperature != 9999 && quality.matches("[01459]"))
      context.write(year, airTemperature)
  }
}

class MaxTemperatureReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  import HadoopConversion._
  import scala.collection.JavaConversions._
  override def reduce( key: Text,  values:Iterable[IntWritable], 
		      context:Reducer[Text, IntWritable, Text, IntWritable]#Context){
      context.write(key, values.reduceLeft(max(_, _)))
      //context.write(key, values.max)
  }

}

class MaxTemperature

object MaxTemperature {
    def main(args: Array[String]) {
	if(args.length != 2) {
	    System.err.println("Usage: MaxTemperature <input path> <output path>");
	    System.exit(-1);
	}

	val job = new Job
	job.setJarByClass(classOf[MaxTemperature])

	FileInputFormat.addInputPath(job, new Path(args(0)));
	FileOutputFormat.setOutputPath(job, new Path(args(1)));

	job.setMapperClass(classOf[MaxTemperatureMapper]);
	job.setReducerClass(classOf[MaxTemperatureReducer]);

	job.setOutputKeyClass(classOf[Text]);
	job.setOutputValueClass(classOf[IntWritable]);

	System.exit(if (job.waitForCompletion(true))  0 else 1)
    }
}

// stop at org.apache.hadoop.conf.Configuration:803
// stop at org.apache.hadoop.mapred.MapTask$MapOutputBuffer:792
// stop at org.apache.hadoop.mapred.JobConf:551
// To Stop: stop in org.apache.hadoop.mapred.MapTask$MapOutputBuffer.collect
// To Run:  run hadooptest.MaxTemperature /home/maeda/hadoop/input/sample.txt /home/maeda/hadoop/output
// stop at org.apache.hadoop.mapred.MapTask$NewOutputCollector:613
