package parte3;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import scala.Serializable;
import scala.Tuple2;
import scala.reflect.io.Streamable;

import javax.xml.crypto.Data;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class ComparadorTuplos implements Comparator<Tuple2<String, BigDecimal>>, Serializable {
    final static ComparadorTuplos INSTANCE = new ComparadorTuplos();
    public int compare(Tuple2<String, BigDecimal> t1, Tuple2<String, BigDecimal> t2) {
        return -t1._2.compareTo(t2._2);
    }
}

class ComparadorTuplosTitles implements Comparator<Tuple2<String, Integer>>, Serializable {
    final static ComparadorTuplosTitles INSTANCE = new ComparadorTuplosTitles();
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
        return -t1._2.compareTo(t2._2);
    }
}

public class Queries {


    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Queries")
                .config("hive.metastore.uris", "thrift://sparktp2-m:9083")
                .enableHiveSupport()
                .getOrCreate();

        // ############################ QUERY 1 #################################
        // ####### Nome e idade from name basics
        //actor, (name , age)
        JavaPairRDD<String, Tuple2<String, Integer>> actors_info = spark.table("name_basics_parquet").toJavaRDD()
                .mapToPair(l -> new Tuple2<>(l.getString(0),
                        new Tuple2<>(l.getString(1), l.isNullAt(2) ? 0 : (l.isNullAt(3) ? 2021 - l.getInt(2) : l.getInt(3) - l.getInt(2)))))
                //.saveAsTextFile("hdfs:///resultado1");
                .cache();

        // ####### Actor Number_of_titles
        //actor, numer_of_titles
        JavaPairRDD<String, Integer> number_of_titles= spark.table("title_principals_parquet").toJavaRDD()
                .filter(l -> l.getString(3).equals("actor")|| l.getString(3).equals("actress") || l.getString(3).equals("self"))
                .mapToPair(l -> new Tuple2<>(l.getString(2), 1))
                .foldByKey(0, Integer::sum)
                .mapToPair(p -> new Tuple2<>(p._1, p._2))
                //.saveAsTextFile("hdfs:///resultado2");
                .cache();

        // ####### Actor anos_atividade
        // title,ator
        JavaPairRDD<String, String> title_by_actor = spark.table("title_principals_parquet").toJavaRDD()
                .filter(l -> l.getString(3).equals("actor")|| l.getString(3).equals("actress") || l.getString(3).equals("self"))
                .mapToPair( l -> new Tuple2<>(l.getString(0),l.getString(2)))
                .cache();

        //title, (inicio,fim)
        JavaPairRDD<String, Tuple2<Integer,Integer>> title_year = spark.table("title_basics_parquet").toJavaRDD()
                .mapToPair( l -> new Tuple2<>(l.getString(0),
                                new Tuple2<>( l.isNullAt(5) ? 2021 : l.getInt(5),l.isNullAt(6) ? 2021 : l.getInt(6))))
                .cache();

        //actor, (inicio,fim)
        JavaPairRDD<String, Tuple2<Integer,Integer>> act_years = title_by_actor.join(title_year) // filme, ator, (inicio,fim)
                .mapToPair( l -> new Tuple2<>(l._2._1,l._2._2)) //ignora filme
                .reduceByKey(
                        (Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>)
                                (i1, i2) -> new Tuple2<>(Math.min(i1._1, i2._1), Math.max(i1._2 , i2._2)))
                //.saveAsTextFile("hdfs:///resultado3");
                .cache();

        // ####### Actor classificacao_media
        //title rating
        JavaPairRDD<String, BigDecimal> title_rating = spark.table("title_ratings_parquet_fixed").toJavaRDD()
                .mapToPair( l -> new Tuple2<>( l.getString(0), l.getDecimal(1)))
                .cache();

        //actor, mean(rating)
        JavaPairRDD<String, Float> actor_class = title_by_actor.join(title_rating).mapToPair( l -> new Tuple2<>(l._2._1, new Tuple2<>(l._2._2,1)))
                .reduceByKey(
                        (Function2<Tuple2<BigDecimal, Integer>, Tuple2<BigDecimal, Integer>, Tuple2<BigDecimal, Integer>>)
                                (i1, i2) -> new Tuple2<>(new BigDecimal(i1._1.floatValue() + i2._1.floatValue()), i1._2+ i2._2))
                .mapToPair(v -> new Tuple2<>(v._1, v._2._1.floatValue() / v._2._2))
                //.saveAsTextFile("hdfs:///resultado4");
                .cache();

        // ############################ QUERY 2 #################################
        //JavaPairRDD<String, BigDecimal> title_rating (title,rating) && JavaPairRDD<String, String> title_by_actor(title,actor)

        JavaPairRDD<String, String> hits = title_by_actor.join(title_rating)
                .mapToPair(l -> new Tuple2<>(l._2._1, new Tuple2<>(l._1,l._2._2))) //actor, (movie,rating)
                .groupByKey()
                .mapValues( v -> {
                    List<Tuple2<String, BigDecimal>> result = new ArrayList<Tuple2<String, BigDecimal>>();
                    v.forEach(result::add);
                    result.sort(new ComparadorTuplos());
                    result = result.subList(0,Integer.min(result.size(),10));
                    StringBuilder sb = new StringBuilder();
                    sb.append("Top 10 Titles: ");
                    for (Tuple2<String,BigDecimal> aux : result)
                        sb.append("("+ aux._1 +","+aux._2+")");
                    return sb.toString();
                })
                //.saveAsTextFile("hdfs:///resultado30");
                .cache();

        // ############################ QUERY 3 #################################
        //usar number_of_titles
        //actor, (name , decade)

        JavaPairRDD<String, Tuple2<String, Integer>> actors_info_decade = spark.table("name_basics_parquet").toJavaRDD()
                .mapToPair(l -> new Tuple2<>(l.getString(0),
                        new Tuple2<>(l.getString(1), l.isNullAt(2) ? 190 : (int) (l.getInt(2)/10))))
                //.saveAsTextFile("hdfs:///resultado1");
                .cache();

        JavaPairRDD<Integer, String> c = actors_info_decade.join(number_of_titles) //(actor, ((nome,decada), number_titles))
                .mapToPair(l -> new Tuple2<>(l._2._1._2, new Tuple2<>(l._2._1._1, l._2._2))) //(decada, (nome, number_titles))
                .groupByKey()
                .mapValues( v -> {
                    List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
                    v.forEach(result::add);
                    result.sort(new ComparadorTuplosTitles());
                    result = result.subList(0,Integer.min(result.size(),10));
                    StringBuilder sb = new StringBuilder();
                    sb.append("Top 10 Decade: ");
                    for (Tuple2<String,Integer> aux : result)
                        sb.append("("+ aux._1 +","+aux._2+")");
                    return sb.toString();
                })
                .cache();

        // actor, String -> List<(Nome, titulos)>
        JavaPairRDD<String, String> query3 = actors_info_decade
                .mapToPair(l -> new Tuple2<>(l._2._2, new Tuple2<>(l._1,l._2._1))) //decade, (actor , name)
                .join(c) // decade, ((actor , name), List<Actors>)
                .mapToPair(l -> new Tuple2<>(l._2._1._1, l._2._2))
                //.saveAsTextFile("hdfs:///resultado1");
                .cache();

        //query3.saveAsTextFile("hdfs:///resultado_query3");

        // ############################ QUERY 4 #################################

        // Actor [Actor]
        JavaPairRDD<String, Iterable<String>> friends = title_by_actor.join(title_by_actor)
                .filter(p -> !p._2._1.equals(p._2._2))
                .mapToPair(p -> p._2)
                .groupByKey()
                //.saveAsTextFile("hdfs:///resultado7")
                .cache();

        // ############################ RSULTADO FINAL #################################
        actors_info.join(number_of_titles).join(act_years).join(actor_class).join(hits).join(query3).join(friends).saveAsTextFile("hdfs:///resultadofinal");
        //actors_info.join(number_of_titles).join(act_years).join(actor_class).join(hits).join(friends).saveAsTextFile("hdfs:///resultadofinal");

    }


}
