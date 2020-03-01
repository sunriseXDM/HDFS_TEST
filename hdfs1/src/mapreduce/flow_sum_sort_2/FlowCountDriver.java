package mapreduce.flow_sum_sort_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {

    //LongWritable, Text, FlowBean_Comparable, Text
    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean_Comparable, Text> {
        //静态对象用于全排序？
        FlowBean_Comparable bean = new FlowBean_Comparable();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = line.split("\t");
            String phoneNbr = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

            bean.set(upFlow, downFlow);
            v.set(phoneNbr);

            // 输出bean, v
            context.write(bean, v);
        }
    }

    //FlowBean_Comparable, Text, Text, FlowBean_Comparable
    static class FlowCountSortReducer extends Reducer<FlowBean_Comparable, Text, Text, FlowBean_Comparable> {

        @Override
        protected void reduce(FlowBean_Comparable bean, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //values.iterator().next()
            context.write(values.iterator().next(), bean);
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);


        job.setJarByClass(FlowCountDriver.class);


        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);


        job.setMapOutputKeyClass(FlowBean_Comparable.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean_Comparable.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));

//      Path outPath = new Path(args[1]);
//		FileSystem fs = FileSystem.get(configuration);
//		if (fs.exists(outPath)) {
//			fs.delete(outPath, true);
//		}
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
