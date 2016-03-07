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


// class CustomSparkListener extends SparkListener {
//     var sumOfTasks: Int = 0
//     var numTreeAggStages: Int = 0
//     var numSamples: Int = 1
//     var changeRDDPartitions: Int = 0
//     val stageName: String = "treeAggregate"
//     var stageIds: Array[Int] = Array()
//     var taskTimes: Array[Long] = Array()
//     var avgStageTimes: Array[Double] = new Array(10)
//     var treeAggStageAvgTimes: Array[Double] = Array()
//     var lastStageTaskAvg: Double = 0
//     var rddData: RDD[Array[Double]] = Spark.sc.emptyRDD

//     def this(rddData: RDD[Array[Double]]) {
//         this()
//         this.rddData = rddData
//         println(s"Listener -> RDD parts: ${this.rddData.partitions.size}, data size: ${this.rddData.count}")
//     }

//     override def onJobStart(jobStart: SparkListenerJobStart) {
//         treeAggStageAvgTimes :+= 10000.toDouble
//     }

//     override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
//         stageIds :+= stageSubmitted.stageInfo.stageId
//         if ((stageSubmitted.stageInfo.name).take(stageName.length) == stageName)
//         {
//             sumOfTasks = sumOfTasks + stageSubmitted.stageInfo.numTasks
//         }
//     }

//     override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
//         val rddPartitions: Int = Spark.sc.getRDDStorageInfo(0).numPartitions
//         val avgTaskTime: Double = (taskTimes.sum).toDouble/sumOfTasks.toDouble

//         if ((stageCompleted.stageInfo.name).take(stageName.length) == stageName && rddPartitions == stageCompleted.stageInfo.rddInfos(0).numPartitions)
//         {
//             val t: Double = (stageCompleted.stageInfo.completionTime.get).toDouble - (stageCompleted.stageInfo.submissionTime.get).toDouble

//             avgStageTimes(numTreeAggStages) = t

//             if (numTreeAggStages > 8)
//             {
//                 val avgOfTenStages: Double = avgStageTimes.sum/(avgStageTimes.size).toDouble
//                 treeAggStageAvgTimes :+= avgOfTenStages

//                 println(s"--> treeAggStageAvgTimes(last): ${treeAggStageAvgTimes(numSamples-1)}, current: ${treeAggStageAvgTimes(numSamples)} [${avgOfTenStages}]")
//                 println(s"--> Avg. of 10 stages: ${avgOfTenStages}, Stages: ${avgStageTimes.size}")
//                 numTreeAggStages = 0
//                 numSamples = numSamples + 1
//             }
//             else
//             {
//                 numTreeAggStages = numTreeAggStages + 1
//             }
//         }
//     }

//     override def onTaskStart(taskStarted: SparkListenerTaskStart): Unit = {
//         taskTimes :+= (0).toLong
//     }

//     override def onTaskEnd(taskEnded: SparkListenerTaskEnd): Unit = {
//         taskTimes(taskEnded.taskInfo.taskId.toInt) = taskEnded.taskInfo.duration
//     }

//     override def onApplicationEnd(appEnded: SparkListenerApplicationEnd): Unit = {
//         val avgTaskTime: Double = (taskTimes.sum).toDouble/sumOfTasks.toDouble
//         println(s"-> Total Task Times: ${taskTimes.sum} [msec], Average task time: ${avgTaskTime} [msec], array size: ${taskTimes.size}, treeAggregate Tasks: ${sumOfTasks} [Stages: ${stageIds.size}].")
//     }
// }

// class movieLensSVD extends java.io.Serializable 
// {
//     val delimiter: String = ","
//     var dataFile: String = ""
//     var data: RDD[Array[Double]] = Spark.sc.emptyRDD

//     def this(dataFile: String)
//     {
//         // this(sc)
//         this()
//         this.dataFile = dataFile
//         println("--> movieLensSVD object instantiated! data file name: " + this.dataFile)
//     }

//     def addListener(): Unit = {
//         val myListener: SparkListener = new CustomSparkListener()
//         Spark.sc.addSparkListener(myListener: SparkListener)
//     }

//     def readData(): Unit = {
//         data = Spark.sc.textFile(this.dataFile).map(line => line.split(delimiter).map(_.toDouble))
//         data.cache()
//         println("readData --> data partitions: " + data.partitions.size)
//         println(s"readData --> data.toDebugString: ${data.toDebugString}")
//     }

//     def rddIncreasePartitions(numNewPartitions: Int): Unit = {
//         data.unpersist()
//         data = data.repartition(numNewPartitions)
//         data.cache()
//     }

//     def rddDecreasePartitions(numNewPartitions: Int): Unit = {
//         data.unpersist()
//         data = data.coalesce(numNewPartitions)
//         data.cache()
//     }

//     def computeMovieLensSVD(): Unit = {
//         // compute SVD
//         val dataCoordMatrix: CoordinateMatrix = new CoordinateMatrix(
//                                                     data.map{case entryVal => 
//                                                         new MatrixEntry(entryVal(0).toLong-1, entryVal(1).toLong-1, entryVal(2).toDouble)}
//                                                                     )
//         println("dataCoordMatrix dimensions: [" + dataCoordMatrix.numRows + " x " + dataCoordMatrix.numCols + "]")
//         val matData: RowMatrix = dataCoordMatrix.toRowMatrix 
//         println("Data converted to RowMatrix.")

//         // find svd of matrix matData
//         println("Starting SVD...")
//         val svd: SingularValueDecomposition[RowMatrix, Matrix] = matData.computeSVD(10, computeU = true)
//     }
// }

// // class mySVDTuner {}
// // class tunerLogic {}
// object Spark
// {
//     val conf = new SparkConf().setAppName("testTunerSVD1")
//                                 .setMaster("spark://oc2343567383.ibm.com:7077")
//                                 .set("spark.cores.max", "2")
//                                 .set("spark.executor.cores", "1")
//                                 .set("spark.executor.memory", "1g")
//     val sc = new SparkContext(conf)
// }

object test_listen
{
    def main(args: Array[String])
    {
        // val file: String = args(0)

        // val testMovieLensSVDObject: movieLensSVD = new movieLensSVD(file: String)
        // testMovieLensSVDObject.readData()
        // testMovieLensSVDObject.addListener()
        // testMovieLensSVDObject.rddIncreasePartitions(8)
        // testMovieLensSVDObject.computeMovieLensSVD()
    }
}