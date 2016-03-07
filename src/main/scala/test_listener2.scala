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


class CustomSparkListener extends SparkListener
{
    val stageName: String = "treeAggregate"
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val rddPartitions: Int = movieLensSVD.getMainRDDNumPartitions()
        
        if ((stageCompleted.stageInfo.name).take(stageName.length) == stageName && rddPartitions == stageCompleted.stageInfo.rddInfos(0).numPartitions)
        {
            val t: Double = (stageCompleted.stageInfo.completionTime.get).toDouble - (stageCompleted.stageInfo.submissionTime.get).toDouble

            movieLensSVD.updateStageTimeArray(t)
        }
    }

    override def onApplicationEnd(appEnded: SparkListenerApplicationEnd): Unit = {
        movieLensSVD.computeStageStats()
    }
}

// class movieLensSVD extends java.io.Serializable
object movieLensSVD
{
    private var dataFile: String = ""
    private var conf: SparkConf = _
    private var sc: SparkContext = _
    private var data: RDD[Array[Double]] = _
    private var maxCores: String = _
    private var execCores: String = _
    private var execMem: String = _

    private var numTreeAggStages: Int = 1
    private var treeAggStageTimes: Array[Double] = Array()

    private def newContext(maxCores: String, execCores: String, execMem: String): Unit = {
        this.conf = new SparkConf().setAppName("testTunerSVD1")
                                .setMaster("spark://oc2343567383.ibm.com:7077")
                                .set("spark.cores.max", maxCores)
                                .set("spark.default.parallelism", maxCores)
                                .set("spark.executor.cores", execCores)
                                .set("spark.executor.memory", execMem)
        this.sc = new SparkContext(conf)
    }

    private def stopSparkContext(): Unit = {
        sc.stop()
    }


    private def addListener(): Unit = {
        val myListener: SparkListener = new CustomSparkListener()
        sc.addSparkListener(myListener: SparkListener)
    }

    private def readData(): Unit = {
        val delimiter: String = ","
        data = sc.textFile(this.dataFile, maxCores.toInt).map(line => line.split(delimiter).map(_.toDouble))
        data.cache()
        println("readData --> data partitions: " + data.partitions.size)
    }

    private def rddIncreasePartitions(numNewPartitions: Int): Unit = {
        data.unpersist()
        data = data.repartition(numNewPartitions)
        data.cache()
    }

    private def rddDecreasePartitions(numNewPartitions: Int): Unit = {
        data.unpersist()
        data = data.coalesce(numNewPartitions)
        data.cache()
    }

    private def computeMovieLensSVD(): Unit = {
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
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = matData.computeSVD(20, computeU = true)
    }

    private def initContext(dataFile: String, maxCores: String, execCores: String, execMem: String): Unit = {
        this.dataFile = dataFile
        this.maxCores = maxCores
        this.execCores = execCores
        this.execMem = execMem
        newContext(this.maxCores, this.execCores, this.execMem)
        println("--> movieLensSVD object instantiated! data file name: " + dataFile)

    }

    def runTest(dataFile: String, maxCores: String, execCores: String, execMem: String): Unit = {
        println(s"* * * * Start Test => [maxCores: ${maxCores}, execCores: ${execCores}, execMem: ${execMem}] * * * *")
        initContext(dataFile, maxCores, execCores, execMem)
        readData()
        addListener()
        // rddIncreasePartitions(8)
        computeMovieLensSVD()
        stopSparkContext()
        println(s"* * * * * * * * * * * * * * * * * * * * Test Completed * * * * * * * * * * * * * * * * * * * * * *")
    }

    def getMainRDDNumPartitions(): Int = {
        sc.getRDDStorageInfo(0).numPartitions
    }

    def computeStageStats(): Unit = {
        val avgStageTime: Double = treeAggStageTimes.sum/numTreeAggStages.toDouble
        println(s"Listener --> avgTreeAggStageTime: ${avgStageTime}, numTreeAggStages: ${numTreeAggStages}")
    }

    def updateStageTimeArray(t: Double): Unit = {
        treeAggStageTimes :+= t
        numTreeAggStages = numTreeAggStages + 1
    }

    def cancelAllJob(): Boolean = {
        var isTrue: Boolean = false
        if (numTreeAggStages > 50)
        {
            isTrue = true
        }
        isTrue
    }
}

// class mySVDTuner {}
// class tunerLogic {}

object test_object
{
    def main(args: Array[String])
    {
        val file: String = args(0)

        movieLensSVD.runTest(file, "1", "1", "1g")
        movieLensSVD.runTest(file, "2", "1", "1g")
        movieLensSVD.runTest(file, "2", "2", "1g")

        println("test_object will now exit!")
    }
}