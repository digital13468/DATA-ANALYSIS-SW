package wikipedia
// This file and dataset were provided by Dr. Heather Miller from her Coursera course, Big Data Analysis with Scala and Spark
import java.io.File

object WikipediaData {

  private[wikipedia] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat")
    if (resource == null) sys.error("Please make sure the dataset is available")
    new File(resource.toURI).getPath
  }

  private[wikipedia] def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    WikipediaArticle(title, text)
  }
}
