package org.houzhizhen.tpcds;

import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class PureJavaTpcdsTest {
    private static String[] names = new String[] {
            "q1","q2","q3","q4","q5","q6","q7","q8","q9","q10",
            "q11","q12","q13","q14a","q14b","q15","q16","q17","q18","q19",
            "q20","q21","q22","q23a","q23b","q24a","q24b","q25","q26","q27",
            "q28","q29","q30","q31","q32","q33","q34","q35","q36","q37",
            "q38","q39a","q39b","q40","q41","q42","q43","q44","q45","q46","q47",
            "q48","q49","q50","q51","q52","q53","q54","q55","q56","q57","q58",
            "q59","q60","q61","q62","q63","q64","q65","q66","q67","q68","q69",
            "q70","q71","q72","q73","q74","q75","q76","q77","q78","q79",
            "q80","q81","q82","q83","q84","q85","q86","q87","q88","q89",
            "q90","q91","q92","q93","q94","q95","q96","q97","q98","q99",
            "ss_max"
    };


    public static void main(String[] args) throws IOException {
        String dbName = "tpcds_sf1_withdecimal_withdate_withnulls";
        if (args.length > 0) {
            dbName = args[0];
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("java_spark_demo")
                .getOrCreate();
        int i = 0;
        try {
            spark.sql("use " + dbName);
            String[] sqls = getContents();
            for (; i < names.length; ++i) {
                System.out.println("begin execute " + names[i]);
                String sql = sqls[i];
                long start = System.currentTimeMillis();
                String group = "benchmark " +  names[i];
                spark.sparkContext().setJobGroup(group, group, true);
                spark.sql(sql).show(100);
                long time = System.currentTimeMillis() - start;
                System.out.println(names[i] + " cost time: " + time);
            }
        } catch (Exception e) {
            System.out.println("execute " + names[i] + " failed");
        } finally {
            spark.stop();
        }
    }

    public static String[] getContents() throws IOException {
        String[] contents = new String[names.length];
        for (int i = 0; i < names.length; ++i) {
            InputStream in = PureJavaTpcdsTest.class.getClassLoader().getResourceAsStream("tpcds_2_4/"+ names[i] + ".sql");
            BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            contents[i] = sb.toString();
            // System.out.println(names[i] + ":" + contents[i]);
        }
        return contents;
    }
}
