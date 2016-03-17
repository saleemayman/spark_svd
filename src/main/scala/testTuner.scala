import scala.collection.mutable.Map
import scala.collection.immutable.ListMap
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
    private var avgStageTime: Double = _
    private var treeAggStageTimes: Array[Double] = Array()

    private def newContext(maxCores: String, execCores: String, execMem: String): Unit = {
        this.conf = new SparkConf().setAppName("testTunerSVD1")
                                .setMaster("spark://konstanz:7077")
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
        data = sc.textFile(this.dataFile).map(line => line.split(delimiter).map(_.toDouble))
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
        val t0: Long = System.nanoTime()
        val dataCoordMatrix: CoordinateMatrix = new CoordinateMatrix(
                                                    data.map{case entryVal => 
                                                        new MatrixEntry(entryVal(0).toLong-1, entryVal(1).toLong-1, entryVal(2).toDouble)}
                                                                    )
        // println("dataCoordMatrix dimensions: [" + dataCoordMatrix.numRows + " x " + dataCoordMatrix.numCols + "]")
        val matData: RowMatrix = dataCoordMatrix.toRowMatrix 
        // println("Data converted to RowMatrix.")

        // find svd of matrix matData
        // println("Starting SVD...")
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = matData.computeSVD(20, computeU = true)
        val t1: Long = System.nanoTime()
        println("SVD computed, time [sec]: " + (t1-t0)/1000000000)
    }

    private def initContext(dataFile: String, maxCores: String, execCores: String, execMem: String): Unit = {
        this.dataFile = dataFile
        this.maxCores = maxCores
        this.execCores = execCores
        this.execMem = execMem
        newContext(this.maxCores, this.execCores, this.execMem)
        // println("--> movieLensSVD object instantiated! data file name: " + dataFile)

    }

    def runTest(dataFile: String, maxCores: String, execCores: String, execMem: String): Double = {
        println(s"* * * * Start Test => [maxCores: ${maxCores}, execCores: ${execCores}, execMem: ${execMem}] * * * *")
        initContext(dataFile, maxCores, execCores, execMem)
        readData()
        addListener()
        rddIncreasePartitions(maxCores.toInt)
        computeMovieLensSVD()
        stopSparkContext()
        println(s"* * * * * * * * * * * * * * * * * * * * Test Completed * * * * * * * * * * * * * * * * * * * * * *")
        avgStageTime
    }

    def getMainRDDNumPartitions(): Int = {
        sc.getRDDStorageInfo(0).numPartitions
    }

    def computeStageStats(): Unit = {
        avgStageTime = treeAggStageTimes.sum/numTreeAggStages.toDouble
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

object tunerLogic
{
    private var dataFile: String = _
    private var maxCores: Int = _
    private var numWorkers: Int = _
    private var numTuningParams: Int = _
    private var execCores: Int = _
    private var execMem: String = "1g"  // hard-coded for now
    private var testScenarios: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Array[Int]]] = Map[String, Map[String, Array[Int]]]()
    private var testScenarioTimes: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Double]] = Map[String, Map[String, Double]]()
    private var unfitScenarios: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, (Array[Int], Double)]] = 
                                                                                                            Map[String, Map[String, (Array[Int], Double)]]()
    private var testScenarioMaxCoresTP: scala.collection.mutable.Map[String, Array[Int]] = Map[String, Array[Int]]()
    var maxCoresTimeThreshold: Double = 0.05

    def initTuning(dataFile: String, tuningParams: Map[String, Int], numWorkers: Int, maxCores: Int): Unit = {
        this.dataFile = dataFile
        this.numWorkers = numWorkers
        this.maxCores = maxCores - (maxCores % 4)
        this.numTuningParams = tuningParams.size

        initScenarios(tuningParams)
        initMaxCoreScenarios()
        executeScenariosFor("max.cores")
        initExecCoreScenarios()
        executeScenariosFor("exec.cores")
    }

    private def initScenarios(tuningParams: Map[String, Int]): Unit = {
        for ((k, v) <- tuningParams)
        {
            testScenarios += (k -> Map[String, Array[Int]]())
            unfitScenarios += (k -> Map[String, (Array[Int], Double)]())
            testScenarioTimes += (k -> Map[String, Double]())
        }
    }

    private def initMaxCoreScenarios(): Unit = {
        var coreStride: Int = 2
        if (maxCores > 4)
        {
            coreStride = 4
        }

        var cores: Int = 32
        var i: Int  = 0
        while (cores > (maxCores * 0.25))
        {
            cores = (maxCores - (coreStride * i)).toInt
            testScenarios("max.cores") += ((i + 1).toString -> Array(cores, cores, 1))
            testScenarioTimes("max.cores") += ((i + 1).toString -> 0.toDouble)
            i = i + 1
        }
    }

    private def initExecCoreScenarios(): Unit = {
        var scenarioMaxCores: Int = maxCores
        var coresPerExec: Int = maxCores
        var i: Int = 0

        for ( (k, v) <- testScenarios("max.cores"))
        {
            scenarioMaxCores = v(0)
            coresPerExec = scenarioMaxCores - 2
            while (coresPerExec > 0)
            {
                testScenarios("exec.cores") += ((i + 1).toString -> Array(scenarioMaxCores, coresPerExec, 1))
                testScenarioTimes("exec.cores") += ((i + 1).toString -> 0.toDouble)
                coresPerExec = coresPerExec - 2
                i = i + 1
                // if (scenarioMaxCores % coresPerExec)
                // {
                    
                // }
            }
        }
    }

    private def executeScenariosFor(tuningParameter: String): Unit = {
        val sortedScenariosToTest: scala.collection.immutable.ListMap[String, Array[Int]] = ListMap(testScenarios(tuningParameter).toSeq.sortBy(_._1):_*)
        for ( (k, v) <- sortedScenariosToTest )
        {
            testScenarioTimes(tuningParameter)(k) = movieLensSVD.runTest(dataFile, v(0).toString, v(1).toString, execMem)
        }
        checkTestResults(tuningParameter)
    }

    private def checkTestResults(tuningParameter: String): Unit = {
        val sortedScenarios: scala.collection.immutable.ListMap[String, Double] = ListMap(testScenarioTimes(tuningParameter).toSeq.sortBy(_._1):_*)
        var minTime: Double = testScenarioTimes(tuningParameter).minBy(_._2)._2
        var changeRatio: Double = 1
        for ( (k, v) <- sortedScenarios )
        {
            changeRatio = 1.0 - minTime/v
            println(s"Test: ${k} ->  Avg. Stage Time: ${v} [msec], changeRatio: ${changeRatio}")

            // prune the scenarios for the current TP
            if (changeRatio > maxCoresTimeThreshold)
            {
                unfitScenarios(tuningParameter) += (k -> (testScenarios(tuningParameter).remove(k).get, testScenarioTimes(tuningParameter).remove(k).get))
            }
        }
    }

    def getTestResults(tuningParameter: String): Unit = {
        val sortedScenarios: scala.collection.immutable.ListMap[String, Double] = ListMap(testScenarioTimes(tuningParameter).toSeq.sortBy(_._1):_*)

        for ( (k, v) <- sortedScenarios )
        {
            println(s"Test: ${k} -> Avg. Stage Time: ${v} [msec], Config: [max.cores: ${testScenarios(tuningParameter)(k)(0)}, exec.cores: ${testScenarios(tuningParameter)(k)(1)}, exec.mem: ${execMem}]")
        }
    }
}

object test_tuner
{

    def main(args: Array[String])
    {
        val numWorkers: Int = 1
        val maxCores: Int = 32
        val file: String = args(0)
        val tuningParams: Map[String, Int] = Map("max.cores" -> 1, "exec.cores" -> 2, "exec.mem" -> 3)

        tunerLogic.initTuning(file, tuningParams, numWorkers, maxCores)
        tunerLogic.getTestResults("max.cores")
        tunerLogic.getTestResults("exec.cores")

        println("test_object will now exit!")
    }
}