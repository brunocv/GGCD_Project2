package parte2;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

class ComparadorTuplosConta implements Comparator<Tuple2<Object, Integer>>, Serializable {
    final static ComparadorTuplosConta INSTANCE = new ComparadorTuplosConta();
    public int compare(Tuple2<Object, Integer> t1, Tuple2<Object, Integer> t2) {
        return -t1._2.compareTo(t2._2);
    }
}

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

        JavaPairRDD<String, Tuple2<Object,Integer>> query1 = spark.table("title_basics_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(8) && !l.isNullAt(5))
                .flatMapToPair(l -> {
                    List<String> f = l.getList(8);
                    return Arrays.stream(f.toArray())
                            .map(g -> new Tuple2<>(new Tuple2<>(g,get_decade(l.getInt(5))), 1)).iterator();
                })
                .reduceByKey((i, j) -> i + j)
                .mapToPair( l -> new Tuple2<>(l._1._2, new Tuple2<>(l._1._1,l._2)))
                .groupByKey()
                .mapValues( v -> {
                    List<Tuple2<Object, Integer>> result = new ArrayList<Tuple2<Object, Integer>>();
                    v.forEach(result::add);
                    result.sort(new ComparadorTuplosConta());
                    return result.get(0);
                })
                .sortByKey(false);

        List<Tuple2<String, Tuple2<Object,Integer>>> query1_result = query1.collect();
        query1_result.forEach(System.out::println);

        // ############################################### QUERY 2 ############################################

        JavaPairRDD<String, Tuple2<Integer,String>> query2_basics = spark.table("title_basics_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(2) && !l.isNullAt(5))
                .mapToPair(l -> new Tuple2<>(l.getString(0), new Tuple2<>(l.getInt(5),l.getString(2))));

        JavaPairRDD<String, BigDecimal> query2_ratings = spark.table("title_ratings_parquet_fixed").toJavaRDD()
                .filter(l -> !l.isNullAt(1))
                .mapToPair(l -> new Tuple2<>(l.getString(0), l.getDecimal(1)));

        JavaPairRDD<Integer, Tuple2<String,BigDecimal>> query2_results = query2_basics.join(query2_ratings) //id,((ano, titulo), rating)
                .mapToPair(l -> new Tuple2<>(l._2._1._1, new Tuple2<>(l._2._1._2, l._2._2))) //ano (titulo,rating)
                .reduceByKey((Function2<Tuple2<String, BigDecimal>, Tuple2<String, BigDecimal>, Tuple2<String, BigDecimal>>)
                       (i1, i2) -> i1._2.floatValue() > i2._2.floatValue() ? i1 : i2)
                .sortByKey(false);

        List<Tuple2<Integer, Tuple2<String, BigDecimal>>> query2_result = query2_results.collect();

        query2_result.forEach(System.out::println);

        // ############################################### QUERY 3 ############################################

        List<Tuple2<String,Integer>> query3 = spark.table("title_principals_parquet").toJavaRDD()
                .filter(l -> !l.isNullAt(3) && (l.getString(3).equals("actor") || l.getString(3).equals("actress") || l.getString(3).equals("self")))
                .mapToPair(l -> new Tuple2<>(l.getString(2), 1))
                .reduceByKey((i, j) -> i + j)
                .mapToPair(l-> l.swap()).sortByKey(false)
                .mapToPair(l-> l.swap())
                .take(10);

        query3.forEach(System.out::println);


    }
}
