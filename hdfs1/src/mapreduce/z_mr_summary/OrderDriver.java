package mapreduce.z_mr_summary;

import mapreduce.inputformat_6.WholeFileInputformat;
import mapreduce.outputformat_7.FilterOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;


public class OrderDriver {
    public static void main(String[] args) throws Exception, IOException {
        // 1 获取配置信息
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        // 2 设置jar包加载路径 setJar("/home/atguigu/wc.jar");
        job.setJarByClass(OrderDriver.class);

        // 3 加载map/reduce类以及输出数据key和value类型
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 6 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 9 小文件合并(大量小文件在HDFS)，如果不设置InputFormat，它默认用的是TextInputFormat.class
        /*
        * 小文件的优化无非以下几种方式：
            （1）在数据采集的时候，就将小文件或小批数据合成大文件再上传HDFS
            （2）在业务处理之前，在HDFS上使用mapreduce程序对小文件进行合并(自定义InputFormat)
            （3）在mapreduce处理时，可采用CombineTextInputFormat提高效率
        * */
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
        //job.setInputFormatClass(WholeFileInputformat.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setOutputFormatClass(FilterOutputFormat.class);

        // 6 加载缓存数据
        job.addCacheFile(new URI("file:///e:/inputcache/pd.txt"));

        // 8 指定需要使用combiner，以及用哪个类作为combiner的逻辑
        job.setCombinerClass(OrderReducer.class);

        // 10 设置reduce端的分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        // 7 设置分区
        job.setPartitionerClass(OrderPartitioner.class);

        // 8 设置reduce个数
        job.setNumReduceTasks(3);

        /* 10 压缩基本原则：
            （1）运算密集型的job，少用压缩
            （2）IO密集型的job，多用压缩
        */
        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式，推荐LZO或者snappy
        conf.setClass("mapreduce.map.output.compress.codec", Lz4Codec.class, CompressionCodec.class);

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式，推荐GZip或者BZiP2
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        // 9 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
