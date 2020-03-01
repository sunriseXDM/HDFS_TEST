package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

public class Hdfs1 {

    public static void main(String[] args) throws Exception{//throws Exception

        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration, "user");

        /*upload*/
        FileInputStream fileInputStream = new FileInputStream(new File("local"));

        FSDataOutputStream  fsDataOutputStream = fileSystem.create(new Path("hdfs"));

        try {
            IOUtils.copyBytes(fileInputStream,fsDataOutputStream,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fileInputStream);
            IOUtils.closeStream(fsDataOutputStream);
        }

        /*download*/
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("hdfs"));

        FileOutputStream fileOutputStream = new FileOutputStream(new File("local"));
        try {
            IOUtils.copyBytes(fileInputStream,fileOutputStream,4096,false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fileOutputStream);
            IOUtils.closeStream(fsDataInputStream);
        }

        fileSystem.close();
    }
}
