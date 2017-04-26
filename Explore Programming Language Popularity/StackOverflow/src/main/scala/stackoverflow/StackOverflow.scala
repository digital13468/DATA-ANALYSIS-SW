package stackoverflow
// The base code was provided by Dr. Heather Miller from her Coursera course, Big Data Analysis with Scala and Spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import annotation.tailrec
import scala.reflect.ClassTag
/**
 * The overall goal is to implement a k-means algorithm which clusters posts on the popular question-answer platform StackOverflow accodring
 * to their score. Moreover, this clustering are executed in parallel for different programming languages, and the results are compared.
 * 
 * The motivation is as follows: StackOverflow is an important source of documentation. However, different user-provided answers may have
 * very different ratings (based on user votes) based on their percieved value. Therefore, we would like to look at the distribution of 
 * questions and their answers. For example, how many highly-rated answers do StackOveflow users post, and how high are their scores? Are
 * there big differences between higher-rated answers and lowver-rated ones?
 * 
 * Finally, we are interested in comparing these distributions for different programmign language communities. Differences in distributions
 * could reflect differences in the availability of documentation. For example, StackOverflow could have better documentation for a certain
 * library than that library's API documentation. However, to avoid invalid conclusions we will focus on the well-defined problem of
 * clustering answers according to their scores.
 * 
 * The data is a CSV (comma-separated values) file with information about StackOverflow posts. Each line in the provided file has the
 * following format:
 * 	<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
 * 
 * A short explanation of the comma-separated fields:
 * 	<postTypeId>:     Type of the post. Type 1 = question, type 2 = answer.
 *  <id>:             Unique id of the post (regardless of type).
 *	<acceptedAnswer>: Id of the accepted answer post. This information is optional, so maybe be missing indicated by an empty string.
 *  <parentId>:       For an answer: id of the corresponding question. For a question:missing, indicated by an empty string.
 *	<score>:          The StackOverflow score (based on user votes).
 *  <tag>:            The tag indicates the programming language that the post is about, in case it's a question, or missing in case it's an 
 *  									answer.
 */
/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) 
  extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  //sc.setLogLevel("WARN") // or ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/scala/stackoverflow/stackoverflow.csv")  // the lines from the csv file as strings
    val raw     = rawPostings(lines)  // the raw Posting entries for each line

    val grouped = groupedPostings(raw)  // questions and answers grouped together

    val scored  = scoredPostings(grouped)  // questions and scores

    val vectors = vectorPostings(scored).persist()  // pairs of (language, score) for each question  


    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together 
   *  
   *  In the raw variable we have simple postins, either questions or answers, but in order to use the data, we need to assemble them
   *  together. Questions are identified using a "postTypeId" == 1, answers to a question with "id" == QID have (a) "postTypeId" == 2 and 
   *  (b) "parentID" == QID.
   *  
   *  To obtain this, in the groupedPostings method, first filter the questions and asnwers separatly and then prepare them for a join
   *  operation by extracting the QID value in the first element of a tuple. Then the last step is to obtain an 
   *  RDD[(QID, Iterable[(Question, Answer)])].  
   *  */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    val questions = postings.filter(posting => posting.postingType == 1).map(question => (question.id, question))
      .partitionBy(new HashPartitioner(20))
    val answers = postings.filter(posting => posting.postingType == 2)
      .map(answer => (answer.parentId.getOrElse(throw new IllegalArgumentException("Parent ID is expected to be defined")), answer))
      .partitionBy(new HashPartitioner(20))
    questions.join(answers).groupByKey()
  }


  /** Compute the maximum score for each posting 
   *  
   *  It returns an RDD containing pairs of (a) questions and (b) the score of the answer with the highest score.
   *  */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    
    grouped.map(qId => (qId._2.toList(0)._1, answerHighScore(qId._2.map(qaPair => qaPair._2).toArray)))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored.map(posting => (firstLangInTag(posting._1.tags, langs).
        getOrElse(throw new IllegalArgumentException("Language is not in the list")) * langSpread, posting._2)).persist()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    // iteratively paring each vector with the index of the closest mean and computing the new means by averaging the values of each cluster
    val newMeans = means.clone()
    val clusters = vectors.map(point => (findClosest(point, means), point)).groupByKey().mapValues(scores => averageVectors(scores))
                    .collect()
    


    for (idx <- 0 until clusters.length) 
      newMeans(clusters(idx)._1) = clusters(idx)._2
    

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length, "a1.length = " + a1.length + ", a2.length = " + a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val langLabel: String   = langs(vs.map(score => (score._1, 1)).groupBy(occurrence => occurrence._1).
          maxBy(count => count._2.size)._1 / langSpread) // most common language in the cluster
      val langPercent: Double = {
        val mostCommonLangCode = vs.map(score => (score._1, 1)).groupBy(occurrence => occurrence._1)
          .maxBy(count => count._2.size)._1
          vs.filter(score => score._1 == mostCommonLangCode).size * 100.0 / vs.size
      } // percent of the questions in the most common language
      val clusterSize: Int    = vs.size  // the size of the cluster
      val medianScore: Int    = {
        if (vs.size % 2 == 1)
          vs.map(score => score._2).toList.sortWith(_ < _)(vs.size / 2)
        else
          (vs.map(score => score._2).toList.sortWith(_ < _)((vs.size / 2) - 1) 
              + vs.map(score => score._2).toList.sortWith(_ < _)(vs.size / 2)) / 2
      }  // the median of the highest answer scores

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
