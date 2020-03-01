package mapreduce.inputformat_6;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class InputFormatDriver {

    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text k = new Text();;

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {

            // 获取切片信息
            InputSplit split = context.getInputSplit();
            // 获取切片路径
            Path path = ((FileSplit) split).getPath();
            // 根据切片路径获取文件名称
            k.set(path.toString());

            // 文件名称为key
            context.write(k, value);
        }
    }

    public static void main(String[] args) throws Exception {
        args = new String[] { "e:/inputinputformat", "e:/output1" };

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(InputFormatDriver.class);
        job.setMapperClass(SequenceFileMapper.class);

        job.setNumReduceTasks(0);

        // job.setInputFormatClass(CombineTextInputFormat.class);
        job.setInputFormatClass(WholeFileInputformat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
