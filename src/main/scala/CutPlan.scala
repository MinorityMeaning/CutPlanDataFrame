import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, window}

import java.io.{File, FileWriter}
import scala.annotation.tailrec
import scala.util.Random

object CutPlan extends App{

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CutPlan")
    .getOrCreate()

  val isNeedPersist = true
  val df1 = applyTransform(new DataFrameGenerator().getDataFrame(0), 20)
  //val df2 = applyTransform(new DataFrameGenerator().getDataFrame(0), 100)
  //val df3 = applyTransform(new DataFrameGenerator().getDataFrame(0), 100)
  //val df4 = applyTransform(new DataFrameGenerator().getDataFrame(0), 100)
  //val df5 = applyTransform(new DataFrameGenerator().getDataFrame(0), 100)

  val transformDf = df1//.union(df2).union(df3).union(df4).union(df5)

  /** Apply various aggregation functions to the DataFrame.
   * This is necessary to increase the DataFrame plan. That is, to increase the data lineage.
   * @param df DataFrame for to increase the data lineage.
   * @param numberOfTransform Number of iterations.
   * @return A dataframe with an enlarged plan.
   */
  @tailrec
  final def applyTransform(df: DataFrame, numberOfTransform: Int): DataFrame = {
    if (numberOfTransform == 0) df
    else{
      val cols = df.columns
      val oldDf = df.select(col(cols(0)), col(cols(1)), col(cols(2)))
                    .filter(col(cols(0)) === Random.nextString(10))
                    .groupBy(col(cols(0))).count
                    .select(col("name_last_name").alias("temp_name"), col("count"))
      val newDf = new DataFrameGenerator().getDataFrame(0)
      val resultDf = newDf.join(oldDf, newDf("name_last_name") === oldDf("temp_name")).drop(col("count"))
      applyTransform(resultDf, numberOfTransform - 1)
    }
  }

    //persist or not persist
  val result_df = if(isNeedPersist) transformDf.persist else transformDf

    // save plans in txt files
  val textPlan = result_df.queryExecution.optimizedPlan.toString
  val jsonPlan = result_df.queryExecution.optimizedPlan.toJSON

  val textFileName = if(isNeedPersist) "persist_plan.txt" else "no_persist_plan.txt"
  val jsonFileName = if(isNeedPersist) "persist_plan_json.txt" else "no_persist_plan_json.txt"
  new File(textFileName)
  new File(jsonFileName)
  val fileWriter1 = new FileWriter(textFileName)
  val fileWriter2 = new FileWriter(jsonFileName)
  fileWriter1.write(textPlan)
  fileWriter2.write(jsonPlan)
  fileWriter1.close()
  fileWriter2.close()
}
