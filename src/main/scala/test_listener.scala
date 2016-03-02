// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.SparkConf
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
// import org.apache.spark.scheduler.{SparListenerStageSubmitted, SparkListener, SparkListenerJobStart}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}


class CustomSparkListener extends SparkListener {
    var sumOfTasks: Int = 0
    val stageName: String = "treeAggregate"
    var stageIds: Array[Int] = Array()
    var taskTimes: Array[Long] = Array()

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        stageIds :+= stageSubmitted.stageInfo.stageId
        if ((stageSubmitted.stageInfo.name).take(stageName.length) == stageName)
        {
            sumOfTasks = sumOfTasks + stageSubmitted.stageInfo.numTasks
        }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val avgTaskTime: Double = (taskTimes.sum).toDouble/sumOfTasks.toDouble
        println(s"Current Total Task Times: ${taskTimes.sum} [msec], Average task time: ${avgTaskTime} [msec], array size: ${taskTimes.size}, treeAggregate Tasks: ${sumOfTasks} [Stages: ${stageIds.size}].")
    }

    override def onTaskStart(taskStarted: SparkListenerTaskStart): Unit = {
        taskTimes :+= (0).toLong
    }

    override def onTaskEnd(taskEnded: SparkListenerTaskEnd): Unit = {
        taskTimes(taskEnded.taskInfo.taskId.toInt) = taskEnded.taskInfo.duration
    }

    override def onApplicationEnd(appEnded: SparkListenerApplicationEnd): Unit = {
        val avgTaskTime: Double = (taskTimes.sum).toDouble/sumOfTasks.toDouble
        println(s"-> Total Task Times: ${taskTimes.sum} [msec], Average task time: ${avgTaskTime} [msec], array size: ${taskTimes.size}, treeAggregate Tasks: ${sumOfTasks} [Stages: ${stageIds.size}].")
    }
}

// class movieLensSVD(sc: SparkContext)
class movieLensSVD extends java.io.Serializable 
{
    val delimiter: String = ","
    var dataFile: String = ""

    def this(dataFile: String)
    {
        // this(sc)
        this()
        this.dataFile = dataFile
        println("--> movieLensSVD object instantiated! data file name: " + this.dataFile)
    }

    // def addListener() {}

    def computeMovieLensSVD()
    {
        // val file: String = args(0)
        // val delimiter: String = ","

        // attach an events listener for the job
        val myListener: SparkListener = new CustomSparkListener()
        Spark.sc.addSparkListener(myListener: SparkListener)

        // do somethign with RDD resulting in actions (to check if the damn listener works)
        val data: RDD[Array[Double]] = Spark.sc.textFile(this.dataFile).map(line => line.split(delimiter).map(_.toDouble))

        // compute SVD
        val dataCoordMatrix: CoordinateMatrix = new CoordinateMatrix(
                                                    data.map{case entryVal => 
                                                        new MatrixEntry(entryVal(0).toLong-1, entryVal(1).toLong-1, entryVal(2).toDouble)}
                                                                    )
        println("dataCoordMatrix dimensions: [" + dataCoordMatrix.numRows + " x " + dataCoordMatrix.numCols + "]")
        val matData: RowMatrix = dataCoordMatrix.toRowMatrix 
        println("Data converted to RowMatrix.")

        // find svd of matrix matData
        println("Starting SVD...")
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = matData.computeSVD(10, computeU = true)
    }
}

// class mySVDTuner {}
// class tunerLogic {}
object Spark
{
    val conf = new SparkConf().setAppName("testTunerSVD1")
                                .setMaster("spark://oc2343567383.ibm.com:7077")
                                .set("spark.cores.max", "4")
                                .set("spark.executor.cores", "1")
                                .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
}

object test_listen
{
    // val conf = new SparkConf().setAppName("testSvd1")
    // val sparkCxt: SparkContext = new SparkContext(conf)

    def main(args: Array[String])
    {
        val file: String = args(0)

        val testMovieLensSVDObject: movieLensSVD = new movieLensSVD(file: String)
        testMovieLensSVDObject.computeMovieLensSVD()

        // println("Computing SVD using an additional Executor!")
        // val testMovieLensSVDObject_2: movieLensSVD = new movieLensSVD(file: String)
        // testMovieLensSVDObject_2.computeMovieLensSVD()
    }
}