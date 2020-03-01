package mapreduce.word_count_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        // 配置提交到yarn上运行,windows和Linux变量不一致
//		configuration.set("mapreduce.framework.name", "yarn");
//		configuration.set("yarn.resourcemanager.hostname", "hadoop103");
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
//		job.setJar("/home/atguigu/wc.jar");
        job.setJarByClass(WordCountDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(configuration);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 分区
        job.setPartitionerClass(WordCountPartitioner.class);
        job.setNumReduceTasks(2);

        // 8 指定需要使用combiner，以及用哪个类作为combiner的逻辑
        job.setCombinerClass(WordCountCombiner.class);

        // 9 小文件合并(大量小文件在HDFS)，如果不设置InputFormat，它默认用的是TextInputFormat.class
        /*
        * 小文件的优化无非以下几种方式：
            （1）在数据采集的时候，就将小文件或小批数据合成大文件再上传HDFS
            （2）在业务处理之前，在HDFS上使用mapreduce程序对小文件进行合并(自定义InputFormat)
            （3）在mapreduce处理时，可采用CombineTextInputFormat提高效率
        * */
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

        /* 10 压缩基本原则：
            （1）运算密集型的job，少用压缩
            （2）IO密集型的job，多用压缩
        */
        // 开启map端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式，推荐LZO或者snappy
        configuration.setClass("mapreduce.map.output.compress.codec", Lz4Codec.class, CompressionCodec.class);

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式，推荐GZip或者BZiP2
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//      FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        // 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
//		job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);


    }
}
