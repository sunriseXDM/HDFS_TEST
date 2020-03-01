package mapreduce.word_count_1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*统计过程中对每一个maptask的输出进行局部汇总，以减小网络传输量即采用Combiner功能*/
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable v :values){
            count += v.get();
        }

        context.write(key, new IntWritable(count));
    }
}
