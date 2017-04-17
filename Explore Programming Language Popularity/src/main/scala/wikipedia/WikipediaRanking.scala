package wikipedia
// Part of this file was provided by Dr. Heather Miller from her Coursera course, Big Data Analysis with Scala and Spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
// We use data from Wikiped to produce a metric of how popular a programming language is.
case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)  // for the sake of simplicity
  
  //override def toString = "{ " + title + " } { " + text + " }" 
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Wiki") // run Spark in "local" mode
  val sc: SparkContext = new SparkContext(conf)
  // The following are test cases
  // test 1
  /*val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
  println(occurrencesOfLang("Java", rdd))*/  // should equal to 1
  // test 2
  /*val langs = List("Scala", "Java")
  val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler")))
  val ranked = rankLangs(langs, rdd)
  println(ranked.head._1)*/  // should be Scala
  // test 3
  /*val langs = List("Scala", "Java")
  val articles = List(WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
    WikipediaArticle("2","Scala and Java run on the JVM"), WikipediaArticle("3","Scala is not purely functional"))
  val rdd = sc.parallelize(articles)
  val index = makeIndex(langs, rdd)
  println(index.count())*/ // should be 2
  // test 4
  /*val langs = List("Scala", "Java")
  val articles = List(WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"), WikipediaArticle("3","Scala is not purely functional"))
  val rdd = sc.parallelize(articles)
  val index = makeIndex(langs, rdd)
  val ranked = rankLangsUsingIndex(index)
  println(ranked.head._1)*/  // should be Scala
  // test 5
  /*val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
  val articles = List(WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"), WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","The cool kids like Haskell more than Java"), WikipediaArticle("5","Java is for enterprise developers"))
  val rdd = sc.parallelize(articles)
  val ranked = rankLangsReduceByKey(langs, rdd)
  println(ranked.head._1)*/  // should be Java
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath, sc.defaultMinPartitions).map(line => WikipediaData.parse(line))
  
  // Compute a ranking of programming languages, using a simple metric: the number of Wikipedia articles that mention language at least once
  /** 
   *  Returns the number of articles on which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(art => art.mentionsLanguage(lang)).aggregate(0)((x, art) => x + 1, (a, b) => a + b)
    /*var start = System.currentTimeMillis()
    val filtered = rdd.filter(art => art.mentionsLanguage(lang))
    var stop = System.currentTimeMillis()
    println(s"filtering of occurrencesOfLang method took ${stop - start} ms for $lang.\n")
    start = System.currentTimeMillis()
    val result = filtered.aggregate(0)((x, art) => x + 1, (a, b) => a + b)
    stop = System.currentTimeMillis()
    println(s"agrregating of occurrencesOfLang method took ${stop - start} ms for $lang.\n")
    result*/
  }
  /*  Use `occurrencesOfLang` to compute the ranking of the languages (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. 
   *     
   *  The output, for example, might look like
   *  List(("Scala",999999),("JavaScript",1278),("LOLCODE",982),("Java",42))
   *  
   *  The list is sorted, where of the pairs, the first component is the name of the language and the second is te number of articles that
   *  mention the language. 
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(pair => -pair._2)
    /*val start = System.currentTimeMillis()
    val result = langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(pair => -pair._2)
    //val result = langs.map(lang => (lang, rdd.filter(art => art.mentionsLanguage(lang)).count().toInt)).sortBy(pair => -pair._2)
    println(result)
    val stop = System.currentTimeMillis()
    println(s"rankLangs method took ${stop - start} ms.\n")
    result*/
  }
  /* Compute an inverted index of the set of articles, mapping each language to the Wikipedia pages in which it occurs.
   * 
   * In our case, an inverted index would be useful for mapping from the names of programming languages to the collection of Wikipedia 
   * articles that mention the name at least onece.
   * 
   * The returned RDD contains pairs such that for each language in the given langs list there is at most one pair.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd.flatMap(art => langs.filter(lang => art.mentionsLanguage(lang)).map(lang => (lang, art))).groupByKey()
    /*val start = System.currentTimeMillis()
    val result = rdd.flatMap(art => langs.filter(lang => art.mentionsLanguage(lang)).map(lang => (lang, art))).groupByKey()
    val stop = System.currentTimeMillis()
    println(s"makeIndex method took ${stop - start} ms.\n")
    result*/
  }
  /*  Compute the language ranking again, but now using the inverted index.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.mapValues(arts => arts.size).sortBy(pair => -pair._2).collect().toList
    /*val start = System.currentTimeMillis()
    val result = index.mapValues(arts => arts.size).sortBy(pair => -pair._2).collect().toList
    println(result)
    val stop = System.currentTimeMillis()
    println(s"rankLangsUsingIndex method took ${stop - start} ms.\n")
    result*/
  }
  /*  Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *  
   *  It is more efficient to compute the ranking directly without first computing an inverted index, in the case where the inverted index
   * from above is only used for computing the ranking and for no other task.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap(art => langs.filter(lang => art.mentionsLanguage(lang)).map(lang => (lang, 1))).reduceByKey((a, b) => a + b)
    .sortBy(pair => -pair._2).collect().toList
    /*val start = System.currentTimeMillis()
    val result = rdd.flatMap(art => langs.filter(lang => art.mentionsLanguage(lang)).map(lang => (lang, 1))).reduceByKey((a, b) => a + b)
    .sortBy(pair => -pair._2).collect().toList
    println(result)
    val stop = System.currentTimeMillis()
    println(s"rankLangsReduceByKey method took ${stop - start} ms.\n")
    result*/
  }

    
  def main(args: Array[String]) {

    
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    val langsRanked4: List[(String, Int)] = timed("Part 4: ranking using inverted index (directly call)", 
        rankLangsUsingIndex(makeIndex(langs, wikiRdd)))
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked, using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))
    

    
    

    

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    println(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
