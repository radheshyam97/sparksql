import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object EmpSalSum {

  def main(args: Array[String]): Unit = {
    val inputFiles = args(0)
    val outputDir = args(1)
    val sparkConf = new SparkConf().setAppName("EmpUtil")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val file = sc.textFile(inputFiles)
    val empRows=file.map(line=>toEmp(line))
    import sqlContext.implicits._
    val empDF=empRows.toDF()
    empDF.registerTempTable("emp_tbl")
    val deptSumDF=sqlContext.sql("SELECT  dept as dept, SUM(sal) FROM emp_tbl GROUP BY dept")
    deptSumDF.write.csv(outputDir)

  }

  case class Emp(empid: Int, empName: String, deg: String, mgrId: Int, joinDt: String, sal: Double, comm: Double, dept: Int)


  def toEmp(row: String): Emp = {

    print("###################3")
    print(row)
    val rowValues = row.split(",")
    val empId = rowValues(0).trim.toInt
    val empName = rowValues(1).trim
    val deg = rowValues(2).trim
    var mgrId:Int=0;
    if (rowValues(3) != null && rowValues(3).trim.size>0)
      mgrId = rowValues(3).trim.toInt

    val joinDt = rowValues(4).trim
    val sal = rowValues(5).trim.toDouble
    var comm: Double = 0.0;
    if (rowValues(6) != null && rowValues(6).trim.size>0)
      comm = rowValues(6).trim.toDouble
    val dept = rowValues(7).trim.toInt
    val e=Emp(empId, empName, deg, mgrId, joinDt, sal, comm, dept)
    e;
  }
}
