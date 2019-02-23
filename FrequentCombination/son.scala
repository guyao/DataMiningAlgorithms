import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scala.tools.nsc.interpreter.Completion.Candidates
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.io.FileWriter
import org.apache.spark.broadcast.Broadcast

object son {
  // Input data path
  var data_path = "data/beauty.csv"

  // Output path
  var output_path = "out"

  // mode: working case
  // 1 for reviewer = (product ...)
  // 2 for product = (reviewer ...)
  var mode = "1"

  var global_candidates: Set[Set[String]] = Set()

  var support = 2

  var global_count: Long = 0

  var partition_number: Int = 0

  def main(args: Array[String]): Unit = {
    // test
    data_path = "data/beauty.csv"
    mode = "1"
    support = 1200
    output_path = "out"


    if (args.length > 0) {
      mode = args(0)
      data_path = args(1)
      support = args(2).toInt
      if (args.length > 3) {
        val option_m = parse_option(Map(), args.slice(3, args.length).toList)
        partition_number = option_m.getOrElse('num_partitions, 0).toString.toInt
        output_path = option_m.getOrElse('output_file, output_path).toString
      }
    }

    val spark = SparkSession
      .builder()
      .appName("Task1")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    var itemRdd = spark
      .read
      .option("header", "true")
      .csv(data_path)
      .rdd

    if (partition_number > 0) {
      itemRdd = itemRdd
        .repartition(partition_number)
    }

    // Init
    global_count = itemRdd.count

    // First MapReduce
    global_candidates = itemRdd
      .mapPartitions(partition_freqent_item)
      .map((_, 1))
      .reduceByKey(_ + _)
      .map({
        case (x, _) =>
          x
      })
      .collect
      .toSet

    // Second MapRedcuce
    val result = itemRdd
      .mapPartitions(partition_count)
      .reduceByKey(_ + _)
      .filter({
        case (k, v) =>
          v >= support
      })

    output(result.collect.toList.map(x => x._1))
//    print_output(result.collect.toList.map(x => x._1))
  }

  type OptionMap = Map[Symbol, Any]

  def parse_option(map : OptionMap, list: List[String]) : OptionMap = {
    def isSwitch(s : String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--num-partitions" :: value :: tail =>
        parse_option(map ++ Map('num_partitions -> value.toInt), tail)
      case string :: opt2 :: tail if isSwitch(opt2) =>
        parse_option(map ++ Map('output_file -> string), list.tail)
      case string :: Nil =>  parse_option(map ++ Map('output_file -> string), list.tail)
    }
  }

  def output(l: List[Set[String]]): Unit = {
    val out = new FileWriter(output_path,true)
    var m = l.groupBy(x => x.size)
    m.keys.toList.sorted.foreach({
      k =>
        out.write(
          m(k).map(
            x =>
              x.toList
                .sorted
                .map(x => "'" + x + "'")
                .mkString("(",", ",")")
          )
          .sorted
          .mkString(", "))
        out.write("\n")
        out.write("\n")
    })
    out.close
  }

  def print_output(l: List[Set[String]]): Unit = {
    var m = l.groupBy(x => x.size)
    m.keys.toList.sorted.foreach({
      k =>
        println(
          m(k).map(
            x =>
              x.toList
                .sorted
                .map(x => "'" + x + "'")
                .mkString("(",", ",")")
          )
            .sorted
            .mkString(", "))
        println()
    })
  }

  def rdd_to_basket(item: Iterator[Row]): (scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]]) = {
    val REVIEWER_ID = 0
    val PRODUCT_ID = 1
    var BASKET_INDEX = 0
    var ITEM_INDEX = 1
    if (mode == "1") {
      // case 1: reviewer = (product1, product2 ... )
      BASKET_INDEX = REVIEWER_ID
      ITEM_INDEX = PRODUCT_ID
    }
    else if (mode == "2") {
      // case 2: product = (reviewer1, reviewer2 ... )
      BASKET_INDEX = PRODUCT_ID
      ITEM_INDEX = REVIEWER_ID
    } else {
      // empty in purpose
    }
    val m = scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]]()
    item.foreach({
      x =>
        var k = x.getString(BASKET_INDEX)
        var v = x.getString(ITEM_INDEX)

        if (m.contains(k)) {
          m(k).add(Set(v))
        } else {
          m(k) = scala.collection.mutable.Set[Set[String]](Set(v))
        }
    })
    m
  }

  def partition_count(item: Iterator[Row]): Iterator[(Set[String], Int)] = {
    val m = rdd_to_basket(item)
    partition_count_basket(m).iterator
  }

  def partition_count_basket(input_basket: scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]]): List[(Set[String], Int)] = {
    var basket = input_basket
    val counter = scala.collection.mutable.Map[Set[String], Int]().withDefaultValue(0)

    var items = baskets_to_items(basket)

    while (items.nonEmpty) {
      items.foreach(
        x=>
          if (global_candidates.contains(x)) {
            counter(x) += 1
          }
      )
      basket = combine_baskets(filter_baskets(basket, global_candidates))
      items = baskets_to_items(basket)
    }
    counter.toList
  }


  def partition_freqent_item(item: Iterator[Row]): Iterator[Set[String]] = {
    val m = rdd_to_basket(item)
    a_priori(m).iterator
  }

  def a_priori(input_basket: scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]]): Set[Set[String]] = {
    var basket = input_basket
    var items = baskets_to_items(basket)
    var sample_support = ((items.size.toDouble / global_count.toDouble) * support).toInt
    var candidates = filter_candidates(items, items.toSet, sample_support)

    def iter_canditates(candidates: Set[Set[String]]): Set[Set[String]] = {
      if (candidates.nonEmpty) {
        basket = combine_baskets(filter_baskets(basket, candidates))
        items = baskets_to_items(basket)
        candidates ++ iter_canditates(filter_candidates(items, combine_candidates(candidates), sample_support))
      }
      else candidates
    }
    iter_canditates(candidates)
  }

  def combine_candidates[A](candidates: Set[Set[A]]): Set[Set[A]] = {
    // combine frequent items
    val k = candidates.toList.head.size + 1
    (for (x <- candidates; y <- candidates) yield x.union(y))
      .filter(x => x.size == k)
  }

  def baskets_to_items[A](basket: scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[A]]]): List[Set[A]] = {
    basket
      .flatMap({
        case (k, v) => v
      })
      .toList
  }

  def combine_items(items: scala.collection.mutable.Set[Set[String]]): scala.collection.mutable.Set[Set[String]] = {
    val k = items.toList.head.size + 1
    (for (x <- items; y <- items) yield x.union(y))
      .filter(x => x.size == k)
  }

  def combine_baskets(basket: scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]]): scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]] = {
    basket
      .filter({
        case (k, v) =>
          v.nonEmpty
      })
      .map({
        case (k, v) =>
          (k, combine_items(v))
      })
  }

  def filter_baskets(basket: scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]], candidates: Set[Set[String]]): scala.collection.mutable.Map[String, scala.collection.mutable.Set[Set[String]]] = {
    basket
      .map({
        case (k, v) =>
          (k, v.intersect(candidates))
      })
  }

  def filter_candidates[A](items: List[Set[A]], candidates: Set[Set[A]], filter_support: Double): Set[Set[A]] = {
    val counter = scala.collection.mutable.Map[Set[A], Int]().withDefaultValue(0)

    items
      .filter(x => candidates.contains(x))
      .foreach(x => {
        counter(x) += 1
      })

    //TO DEBUG
    /*
    println(filter_support)
    println(counter
      .filter({
        case (k, v) =>
          v >= filter_support
      })
    )
    println()
    */

    counter
      .filter({
        case (k, v) =>
          v >= filter_support
      })
      .keys
      .toSet
  }
}
