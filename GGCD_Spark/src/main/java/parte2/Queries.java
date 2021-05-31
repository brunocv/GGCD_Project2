package parte2;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class Queries {

    public static String get_decade(int n){

        int dec = -1;
        dec = n - (n%10);
        return String.valueOf(dec);
    }


    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Queries")
                .config("hive.metastore.uris","thrift://sparktp2-m:9083")
                .enableHiveSupport()
                .getOrCreate();

        // ############################################### QUERY 1 ############################################

        JavaPairRDD<Object, Object> query1 = spark.table("title_basics_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(8) && !l.isNullAt(5))
                .flatMapToPair(l -> {
                    List<String> f = l.getList(8);
                    return Arrays.stream(f.toArray())
                            .map(g -> new Tuple2<>(new Tuple2<>(g,get_decade(l.getInt(5))), 1)).iterator();
                })
                .reduceByKey((i, j) -> i + j)
                .mapToPair(l -> l.swap()).sortByKey(false)
                .mapToPair(l -> new Tuple2<>(l._2()._2(), new Tuple2<>(l._2()._1(),l._1())))
                .groupByKey().sortByKey(false)
                .mapToPair(l -> new Tuple2<>(l._1(), Iterables.get(l._2(),0)));

        List<Tuple2<Object, Object>> query1_result = query1.collect();
        query1_result.forEach(System.out::println);

        // ############################################### QUERY 2 ############################################

        JavaPairRDD<String, Integer> query2_basics = spark.table("title_basics_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(5))
                .mapToPair(l -> new Tuple2<>(l.getString(0), l.getInt(5)));

        JavaPairRDD<String, BigDecimal> query2_ratings = spark.table("title_ratings_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(1))
                .mapToPair(l -> new Tuple2<>(l.getString(0), l.getDecimal(1)));

        JavaPairRDD<Integer, Object> query2_best = query2_basics.join(query2_ratings)
                .mapToPair(l -> new Tuple2<>(l._2()._2(), new Tuple2<>(l._2()._1(), l._1())))
                .sortByKey(false)
                .mapToPair(l -> new Tuple2<>(l._2()._1(), new Tuple2<>(l._1(), l._2()._2())))
                .groupByKey().sortByKey(false)
                .mapToPair(l -> new Tuple2<>(l._1(),Iterables.get(l._2(),0)));

        List<Tuple2<Integer, Object>> query2_result = query2_best.collect();

        query2_result.forEach(System.out::println);

        // ############################################### QUERY 3 ############################################

        List<Tuple2<String,Integer>> query3 = spark.table("title_principals_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(3) && (l.getString(3).equals("actor") || l.getString(3).equals("actress")))
                .mapToPair(l -> new Tuple2<>(l.getString(2), 1))
                .reduceByKey((i, j) -> i + j)
                .mapToPair(l-> l.swap()).sortByKey(false)
                .mapToPair(l-> l.swap())
                .take(10);

        query3.forEach(System.out::println);






    }
}
