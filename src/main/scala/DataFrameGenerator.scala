import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random

class DataFrameGenerator(implicit val spark: SparkSession) {

  val name = Array("Вася", "Петя", "Никита", "Аня", "Миша" , "Саша", "Таня", "Борис", "Ахмед", "Брюс",
                    "Тим", "Илья", "Дартаньян", "Шпрот", "Мрот", "Крот", "Марат", "Умалат", "Зина", "Лариса")

  val lastName = Array("Сидоров", "Меченый", "Ряженый", "Калеченный", "Поддатый", "Иванова", "Иванов", "Красный",
                        "Путин", "Чайковских", "Бражелончук", "Прокопчук", "Зеленских", "Рыбова", "Чуточких", "Ким")

  val role = Array("младший", "средний", "старший", "охранник", "не охранник", "вахтёр", "аналитик", "программист",
  "водитель", "кондуктор", "инструктор", "кинолог", "археолог", "юрист", "адвокат", "маргинал", "пенсионер", "спортсмен",
  "аквамен", "супермен", "тракторист", "машинист", "геодезист", "повар", "специалист", "руководитель", "менеджер", "сапёр",
  "балерина", "стюардесса")

  val schema = StructType(
      StructField("name_last_name",StringType,true)::
      StructField("role",StringType,true)          ::
      StructField("experience",IntegerType,true)   ::
      StructField("born", IntegerType, true)       ::
      StructField("salary", IntegerType, true)     :: Nil
  )

  def getDataFrame(recordCount: Int): DataFrame = {

    val data = Range(0, recordCount).map(_ => Row(getName, getRole, getExperience, getBorn, getSalary))

    spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  }

  def getName: String = Random.nextString(25)
  def getRole: String = role(Random.nextInt(role.length))
  def getExperience: Int = Random.nextInt(40)
  def getBorn: Int = 1910 + Random.nextInt(90)
  def getSalary: Int = 10000 + Random.nextInt(100000)

}
