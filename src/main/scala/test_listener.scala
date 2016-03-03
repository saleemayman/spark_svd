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
    var numTreeAggStages: Int = 0
    val stageName: String = "treeAggregate"
    var stageIds: Array[Int] = Array()
    var taskTimes: Array[Long] = Array()
    var avgStageTimes: Array[Double] = new Array(10)
    var lastStageTaskAvg: Double = 0

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        stageIds :+= stageSubmitted.stageInfo.stageId
        if ((stageSubmitted.stageInfo.name).take(stageName.length) == stageName)
        {
            sumOfTasks = sumOfTasks + stageSubmitted.stageInfo.numTasks
        }

        // if (stageSubmitted.stageInfo.stageId == 0)
        // {
        //     avgStageTimes :+= 0.toDouble
        // }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val rddPartitions: Int = Spark.sc.getRDDStorageInfo(0).numPartitions
        val avgTaskTime: Double = (taskTimes.sum).toDouble/sumOfTasks.toDouble
        // lastStageTaskAvg = avgStageTimes.last
        // avgStageTimes :+= avgTaskTime
        // println(s"Current ttl TT: ${taskTimes.sum} [msec], Avg. TT: ${avgTaskTime} [msec], last Avg. ${lastStageTaskAvg}, array size: ${taskTimes.size}, treeAggregate Tasks: ${sumOfTasks} [Stages: ${stageIds.size}].")

        // if (stageCompleted.stageInfo.stageId > 50 && avgTaskTime < (lastStageTaskAvg - 10))
        // {
        //     // val isKilled: Boolean = Spark.sc.killExecutor("0")
        //     println(s"--> Stage: ${stageCompleted.stageInfo.stageId}, Last Avg: ${avgTaskTime}, newAvg: ${lastStageTaskAvg}.")
        // }
        if ((stageCompleted.stageInfo.name).take(stageName.length) == stageName && rddPartitions == stageCompleted.stageInfo.rddInfos(0).numPartitions)
        {
            val t: Double = (stageCompleted.stageInfo.completionTime.get).toDouble - (stageCompleted.stageInfo.submissionTime.get).toDouble
            avgStageTimes(numTreeAggStages) = stageCompleted.stageInfo.completionTime.get
            println(s"--> TreeAgg stages: ${numTreeAggStages}, Stage: ${stageCompleted.stageInfo.name}, Stage time: ${t}")

            if (numTreeAggStages > 8)
            {
                val avgOfTenStages: Double = avgStageTimes.sum/(avgStageTimes.size).toDouble
                // println(s"--> Avg. of 10 stages: ${avgOfTenStages}, Stages: ${avgStageTimes.size}")
                numTreeAggStages = 0
            }
            else
            {
                numTreeAggStages = numTreeAggStages + 1
            }
        }
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
        // attach an events listener for the job
        val myListener: SparkListener = new CustomSparkListener()
        Spark.sc.addSparkListener(myListener: SparkListener)

        // do somethign with RDD resulting in actions (to check if the damn listener works)
        val data: RDD[Array[Double]] = Spark.sc.textFile(this.dataFile, 8).map(line => line.split(delimiter).map(_.toDouble))

        // compute SVD
        data.cache()
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
                                .set("spark.cores.max", "2")
                                .set("spark.executor.cores", "1")
                                .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
}

object test_listen
{
    def main(args: Array[String])
    {
        val file: String = args(0)

        val testMovieLensSVDObject: movieLensSVD = new movieLensSVD(file: String)
        testMovieLensSVDObject.computeMovieLensSVD()
    }
}