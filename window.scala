package examples.ingest.transform

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Archian53 on 2/2/16.
  */
object Window {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WinFuncApp")
    val sc = new SparkContext(conf)
    implicit val hc = new HiveContext(sc)
    import hc.implicits._

    val emp = sc.parallelize(Seq((7369,"SMITH","CLERK",7902,"17-Dec-80",800,20,10),
      (7499,"ALLEN","SALESMAN",7698,"20-Feb-81",1600,300,30),
      (7521,"WARD","SALESMAN",7698,"22-Feb-81",1250,500,30),
      (7566,"JONES","MANAGER",7839,"2-Apr-81",2975,0,20),
      (7654,"MARTIN","SALESMAN",7698,"28-Sep-81",1250,1400,30),
      (7698,"BLAKE","MANAGER",7839,"1-May-81",2850,0,30),
      (7782,"CLARK","MANAGER",7839,"9-Jun-81",2450,0,10),
      (7788,"SCOTT","ANALYST",7566,"19-Apr-87",3000,0,20),
      (7839,"KING","PRESIDENT",0,"17-Nov-81",5000,0,10),
      (7844,"TURNER","SALESMAN",7698,"8-Sep-81",1500,0,30),
      (7821,"ETHAN","SALESMAN",7698,"11-Sep-85",1200,0,30),
      (7876,"ADAMS","CLERK",7788,"23-May-87",1100,0,20))).toDF("empno","name","job","mgr","hdate","sal","comm","dept")

    //emp.printSchema()
    //emp.show()

    emp.registerTempTable("empd")

    //ranking - normal//
    //val erank = hc.sql("SELECT empno,dept,sal,RANK() OVER (PARTITION BY dept ORDER BY sal desc) as rank FROM empd")
    //ranking - dense//
    //val edenserank = hc.sql("SELECT empno,dept,sal,DENSE_RANK() OVER (PARTITION BY dept ORDER BY sal desc) as drank FROM empd")
    //row number//
    val erownum = hc.sql("SELECT ROW_NUMBER() OVER (PARTITION BY mgr ORDER BY empno asc) as rnum, empno, dept, mgr FROM empd")
    //sum a column//
    //val esum = hc.sql("SELECT empno,dept,sal,sum(sal) OVER (PARTITION BY dept ORDER BY sal) as running_total FROM empd")
    //lead - pushing a column up//
    //val elead = hc.sql("SELECT empno,dept,sal,lead(sal) OVER (PARTITION BY dept ORDER BY sal) as next_val FROM empd")
    //lag - pushing a column down//
    //val elag = hc.sql("SELECT empno,dept,sal,lag(sal) OVER (PARTITION BY dept) as prev_val FROM empd")
    //last_value
    //val elast = hc.sql("SELECT empno,dept,sal,last_value(sal) OVER (PARTITION BY dept) as prev_val FROM empd")
    //first_value
    //val efirst = hc.sql("SELECT empno,dept,sal,first_value(sal) OVER (PARTITION BY dept) as prev_val FROM empd")


    //erank.show()
    //edenserank.show()
    //edenserank.show(2)
    erownum.show()
    //esum.show()
    //elead.show()
    //elag.show()
    //elast.show()
    //efirst.show()

    sc.stop()
  }

}
