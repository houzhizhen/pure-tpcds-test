package org.houzhizhen.tpcds;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class ExecuteSqlFile {
    public static void main(String[] args) throws IOException {
        String fileName = null;
        for (int i = 0; i < args.length; ++i) {
            if ("-f".equals(args[i])) {
                fileName = args[++i];
            }
        }
        if (fileName==null) {
            System.out.println("usage: -f sql_file");
            return;
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("Execute Sql File")
                .getOrCreate();
        try {
            String[] sqls = getSqls(new Path(fileName));
            for (int i = 0; i < sqls.length; ++i) {
                String sql = sqls[i];
                if (sql.trim().length() < 6) { // must has 'select'
                    continue;
                }
                System.out.println("begin execute sql: " + sql);
                long start = System.currentTimeMillis();
                spark.sql(sql).show(100);
                long time = System.currentTimeMillis() - start;
                System.out.println("end execute sql:" + sql + " cost time: " + time);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
    public static String[] getSqls(Path path) throws IOException {
        // Can not close fs, because the fs may be used by other part
        FileSystem fs = FileSystem.get(path.toUri(), new org.apache.hadoop.conf.Configuration());

        InputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty() && !line.startsWith("--")) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString().split(";");
    }
}
