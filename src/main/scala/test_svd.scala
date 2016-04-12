import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}


class mySparkListener extends SparkListener
{
    val stageName: String = "treeAggregate"
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val rddPartitions: Int = test_svd.getMainRDDNumPartitions()
        
        if ((stageCompleted.stageInfo.name).take(stageName.length) == stageName && rddPartitions == stageCompleted.stageInfo.rddInfos(0).numPartitions)
        {
            val t: Double = (stageCompleted.stageInfo.completionTime.get).toDouble - (stageCompleted.stageInfo.submissionTime.get).toDouble

            test_svd.updateStageTimeArray(t)
        }
    }

    override def onApplicationEnd(appEnded: SparkListenerApplicationEnd): Unit = {
        test_svd.computeStageStats()
    }
}

object test_svd
{
    val GIGS: Long = 1000000000
    var avgStageTime: Double = _
    var numTreeAggStages: Int = 1
    var treeAggStageTimes: Array[Double] = Array()
    
    // init spark conctext
    val conf = new SparkConf().setAppName("testSvd1")
    val sc = new SparkContext(conf)

    def getMainRDDNumPartitions(): Int = {
        sc.getRDDStorageInfo(0).numPartitions
    }

    def updateStageTimeArray(t: Double): Unit = {
        treeAggStageTimes :+= t
        numTreeAggStages = numTreeAggStages + 1
    }

    def computeStageStats(): Unit = {
        avgStageTime = treeAggStageTimes.sum/numTreeAggStages.toDouble
        println(s"Listener --> avgTreeAggStageTime: ${avgStageTime}, numTreeAggStages: ${numTreeAggStages}")
    }

    def main(args: Array[String])
    {
        val t0: Long = System.nanoTime()    // timing variables    
        var t1: Long = 0
        var t2: Long = 0

        // read input data file name
        val file: String = args(0)
        val delimiter: String = args(1) 
        val numPartitions: Int = args(2).toInt 
        val numSingularValues: Int = args(3).toInt 
        println("Input file name: " + file)

        val myListener: SparkListener = new mySparkListener()
        sc.addSparkListener(myListener: SparkListener)

        // read the data as an RDD
        t1 = System.nanoTime()
        val data1: RDD[Array[Double]] = sc.textFile(file).map(line => line.split(delimiter).map(_.toDouble))
        //val data: RDD[Array[Double]] = sc.textFile(file, numPartitions).map(line => line.split(delimiter).map(_.toDouble)).cache()
        println("1. RDD num. of Partitions: " + data1.partitions.size)
        //val data: RDD[Array[Double]] = data1.coalesce(numPartitions, false).cache()
        val data: RDD[Array[Double]] = data1.repartition(numPartitions).cache()

        t2 = System.nanoTime()
        println("File-to-RDD time [sec]: " + (t2-t1)/GIGS)
        println("2. RDD num. of Partitions: " + data.partitions.size)
        println("Number of entries: " + data.count)
        println("Number of cols per entry: " + data.first().size)

        // create RowMatrix
        t1 = System.nanoTime()
        val dataCoordMatrix: CoordinateMatrix = new CoordinateMatrix(
                                                    data.map{case entryVal => 
                                                        new MatrixEntry(entryVal(0).toLong-1, entryVal(1).toLong-1, entryVal(2).toDouble)}
                                                                    )
        println("dataCoordMatrix dimensions: [" + dataCoordMatrix.numRows + " x " + dataCoordMatrix.numCols + "]")
        val matData: RowMatrix = dataCoordMatrix.toRowMatrix 
        t2 = System.nanoTime()
        println("Data converted to RowMatrix, time [sec]: " + (t2-t1)/GIGS)

        // find svd of matrix matData
        println("Starting SVD...")
        t1 = System.nanoTime()
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = matData.computeSVD(numSingularValues, computeU = true)
        t2 = System.nanoTime()
        println("SVD computed, time [sec]: " + (t2-t1)/GIGS)

        println("Total program run-time [sec]: " + (t2-t0)/GIGS)
        // println("Driver Mem. [MB]: " + (conf.getSizeAsBytes("spark.driver.memory").toDouble/(1024*1024)))
        // println("Driver cores: " + (conf.getOption("spark.driver.cores")))
        // println("Executor cores: " + (conf.getOption("spark.executor.cores")))
    }
}
