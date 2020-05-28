import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Smth {

  def main(args: Array[String]): Unit = {
    //val logFile = "C:/Spark/readme.md"
    val conf = new SparkConf().setAppName("CounterDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(name = "TestApp").getOrCreate()
    val dataFile = spark.read.format("com.databricks.spark.csv")
      .option("header", true).load("C:\\Users\\ideyk\\IdeaProjects\\Lab10\\russian_demography.csv")
    dataFile.createOrReplaceTempView("Demography")
    // Выводим самые плодородные годы/регионы, показываются верхние 20
    spark.sql("SELECT year, region FROM Demography ORDER BY birth_rate DESC").show()
    // Статистика рождаемости/смертности по Москве
    spark.sql("SELECT year, birth_rate, death_rate FROM Demography WHERE region='Moscow'").show()
    // Средняя смертность за 1997 год
    spark.sql("SELECT AVG(death_rate) FROM Demography WHERE year=1997").show()
    spark.stop()

  }

}
