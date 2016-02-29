import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
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
    // override def onJobStart(jobStart: SparkListenerJobStart) {
    //     println(s"-> Job started with ${jobStart.stageInfos.size} stages: $jobStart")
    // }

    var sumOfTasks: Int = 0
    val stageName: String = "treeAggregate"
    var stageIds: Array[Int] = Array()
    var taskTimes: Array[Long] = Array()

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

        stageIds :+= stageSubmitted.stageInfo.stageId
        if ((stageSubmitted.stageInfo.name).take(stageName.length) == stageName)
        {
            sumOfTasks = sumOfTasks + stageSubmitted.stageInfo.numTasks
            // println(s"Stage: ${stageSubmitted.stageInfo.stageId} Tasks: ${stageSubmitted.stageInfo.numTasks}")
        }
    }

    override def onTaskStart(taskStarted: SparkListenerTaskStart): Unit = {
       // println(s"-> Task ${taskStarted.taskInfo.taskId} [Stage Id: ${taskStarted.stageId}] started at ${taskStarted.taskInfo.launchTime} with a ${taskStarted.taskInfo.finishTime} finish time.")

        taskTimes :+= (0).toLong
    }

    override def onTaskEnd(taskEnded: SparkListenerTaskEnd): Unit = {
        // println(s"-> Task ${taskEnded.taskInfo.taskId} launched at ${taskEnded.taskInfo.launchTime} with a ${taskEnded.taskInfo.duration} duration and finshed at ${taskEnded.taskInfo.finishTime}. Task type: ${taskEnded.taskType}.")
        
        taskTimes(taskEnded.taskInfo.taskId.toInt) = taskEnded.taskInfo.duration

        // if ( taskEnded.stageId == stageIds(taskEnded.stageId) )
        // {
        //    println(s"Stage: ${taskEnded.stageId} Task: ${taskEnded.taskInfo.taskId}")
        //    taskTimes(taskEnded.taskInfo.taskId.toInt) = taskEnded.taskInfo.duration
        // }

        // println("--> Task duration: ${taskTimes(taskEnded.taskInfo.taskId)} [msec]")
    }

    override def onApplicationEnd(appEnded: SparkListenerApplicationEnd): Unit = {
        val avgTaskTime: Double = (taskTimes.sum).toDouble/sumOfTasks.toDouble
        println(s"Total Task Times: ${taskTimes.sum} [msec], Average task time: ${avgTaskTime} [msec], array size: ${taskTimes.size}, treeAggregate Tasks: ${sumOfTasks} [Stages: ${stageIds.size}].")
    }

    // def processTaskTimes(): Double = {
    //     val avgTaskTime: Double = (taskTimes.sum).toDouble/(taskTimes.size).toDouble
    //     println(s"Average task time: ${avgTaskTime} [msec]")

    //     avgTaskTime
    // }
}


object test_listen
{
    val conf = new SparkConf().setAppName("testSvd1")
    val sc = new SparkContext(conf)

    def main(args: Array[String])
    {
        val file: String = args(0)
        val delimiter: String = ","

        // attach an events listener for the job
        val myListener: SparkListener = new CustomSparkListener()
        sc.addSparkListener(myListener: SparkListener)

        // do somethign with RDD resulting in actions (to check if the damn listener works)
        val data: RDD[Array[Double]] = sc.textFile(file).map(line => line.split(delimiter).map(_.toDouble))
        // data.count
        // data.first().size

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