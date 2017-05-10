package timeusage
// The base code was provided by Dr. Heather Miller from her Coursera course, Big Data Analysis with Scala and Spark
import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._
/** The sample data was extracted from the data on http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv, which is provided by Kaggle and is
 *  documented at https://www.kaggle.com/bls/american-time-use-survey. It contains information about how do people spend their time (e.g.,
 *  sleeping, eating, working, etc.). The dataset can also be viewed on the same website.
 *  
 *  The goal is to identify three groups of activities: primary needs (sleeping and eating), work, and other(leisure). To observe how do 
 *  people allocate their time between these three kinds of activities, and to see differecnes between men and women, employed and unemployed
 *  people, and young (less than 22 years old), active (between 22 and 25 years old) and elder people.
 *  
 *  Based on the dataset, we want to answer questions like the following:
 *  	how much time do we spend on primary needs compared to other activities?
 *  	do women and men spend the same amount of time in working? 
 *  	does the time spent on primary needs change when people get older?
 *  	how much time do employed people spend on leisure compared to unemployed people?
 *  To achieve this, first read the dataset with Spark, transform it into an intermediate dataset which will be easier to work with for 
 *  our case, and finally compute information that will answer the above questions.
 */

/** Main class */
object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/sample.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()
    timeUsageGroupedSql(summaryDf).show()
    timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf)).show()
  }
  /** The simplest way to create a DataFrame consists in reading a file and letting Spark-sql infer the underlying schema. However, this
   *  approach does not work well with CSV files, because the inferred column types are always String.
   *  
   *  The first column contains a String value identifying the respondent but all the other columns contain numeric values. Since th schema
   *  will not be correctly inferred by Spark-sql, we will define it programmatically, relying on the fact that the first line contains the
   *  name of all the columns of the dataset.
   *  
   *  The dfSchema method turns the first line into a Spark-sql StructType describing the schema of the CSV file, where the first column has
   *  type StringType and all the others have type DoubleType. None of these columns are nullable.
   *  
   *  The row method effectively returns each line into a Spark-sql Row containing columns that matches the schema returned by dfSchema.
   */
  /** @return The read DataFrame along with its column names. */
  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /** @return The schema of the DataFrame, assuming that the first given column has type String and all the others
    *         have type Double. None of the fields are nullable.
    * @param columnNames Column names of the DataFrame
    */
  def dfSchema(columnNames: List[String]): StructType = 
    StructType(columnNames.take(1).map(fieldName => StructField(fieldName, StringType, nullable = false)) 
        ::: columnNames.drop(1).map(fieldName => StructField(fieldName, DoubleType, nullable = false)))


  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = 
    Row.fromSeq((for (i <- line.indices) yield (i, line(i))).toList.map(value => if (value._1 != 0) value._2.toDouble else value._2))
  /** The initial dataset contains lots of information. This level of detail is beyond what we are interested in, the three activities. 
   *  To transform the initial dataset, columns related to the same activity are identified based on the description of the activeity
   *  corresponding to each column (given in https://www.bls.gov/tus/lexiconnoex0315.pdf).
   *  
   *  The classifiedColumns method classifies the given list of column names into three groups. This method returns a triplet containing the
   *  columns lists.
   *  
   *  The timeUsageSummary method projects the detailed dataset into a summarized dataset. This summary contains only 6 columns.
   *  
   *  Each "activity column" contains the sum of the columns related to the same activity of the initial dataset. 
   *  
   *  The columns describing the work status, the sex, and the age contain simplified information.
   */
  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val primaryColumnsStartsWith = List("t01", "t03", "t11", "t1801", "t1803")
    val workingColumnsStartsWith = List("t05", "t1805")
    val otherColumnsStartsWith = List("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")
    val partitionedColumns = columnNames.groupBy(columnName => if (primaryColumnsStartsWith.exists(name => columnName.startsWith(name)))  
                                                                 "primary"
                                                               else if (workingColumnsStartsWith.exists(name => columnName.startsWith(name)))
                                                                 "working"
                                                               else if (otherColumnsStartsWith.exists(name => columnName.startsWith(name))) 
                                                                 "other"
                                                ).mapValues(columnNameList => columnNameList.map(columnName => col(columnName)))
    (partitionedColumns("primary"), partitionedColumns("working"), partitionedColumns("other"))                                                           
  }

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame should have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame excludes people that are not employable (ie telfs = 5).
    *
    * Note that the initial DataFrame contains time in ''minutes''.
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame = {
    val workingStatusProjection: Column = when(df("telfs") >= 1 && df("telfs") < 3, "working").otherwise("not working").as("working")
    val sexProjection: Column = when(df("tesex") === 1, "male") otherwise "female" as "sex"
    val ageProjection: Column = when(df("teage") >= 15 && df("teage") <= 22, "young").when(df("teage") >= 23 && df("teage") <= 55, "active") 
                                  .otherwise("elder") as "age"

    val primaryNeedsProjection: Column = primaryNeedsColumns.reduce(_ + _) / 60.0 as "primary"
    val workProjection: Column = workColumns.reduce(_ + _) / 60.0 as "work"
    val otherProjection: Column = otherColumns.reduce(_ + _) / 60.0 as "other"
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force
  }
  /** To compare the average time spent on each activity, for all the combinations of working status, sex and age, the timeUsageGrouped
   *  method computes the average number of hours spent on each activity, grouped by working status, sex and age, and also ordered.
   *   
   */
  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame should have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age,
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age,
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age.
    *
    * Finally, the resulting DataFrame is sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame = 
    summed.groupBy("working", "sex", "age").avg("primary", "work", "other")
          .select($"working", $"sex", $"age", round($"avg(primary)", 1), round($"avg(work)", 1), round($"avg(other)", 1))
          .orderBy("working", "sex", "age")
  /** Alternative ways to manipulate data: using a plain SQL query, and typed Datasets. The timeUsageSummaryTyped method converts a DataFrame
   *  into a DataSet.
   */
  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))
  }

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    "SELECT working, sex, age, Round(avg_primary, 1), Round(avg_work, 1), Round(avg_other, 1) " +
    "FROM (SELECT working, sex, age, AVG(primary) AS avg_primary, AVG(work) AS avg_work, AVG(other) AS avg_other " +
    f"FROM ${viewName} GROUP BY working, sex, age ORDER BY working, sex, age)"

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] = 
    timeUsageSummaryDf.map(row => TimeUsageRow(row.getAs("working"), row.getAs("sex"), row.getAs("age"), row.getAs("primary"), 
                                                row.getAs("work"), row.getAs("other")))

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed.{
      avg => typedAvg
    }
    summed.groupByKey(row => (row.working, row.sex, row.age))
          .agg(typedAvg[TimeUsageRow](_.primaryNeeds).name("primaryNeeds"), typedAvg[TimeUsageRow](_.work).name("work"), 
                typedAvg[TimeUsageRow](_.other).name("other"))
          .select($"key._1" as "working", $"key._2" as "sex", $"key._3" as "age", round($"primaryNeeds", 1) as "primaryNeeds", 
              round($"work", 1) as "work", round($"other", 1) as "other")
          .orderBy("working", "sex", "age")
          .map(row => TimeUsageRow(row.getAs("working"), row.getAs("sex"), row.getAs("age"), row.getAs("primaryNeeds"), row.getAs("work"), 
              row.getAs("other")))

  }
}

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)