import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object testSvd_taxiData
{
    val GIGS: Long = 1000000000
    val matRows: Int = 200000
    val matCols: Int = 200000
    val r1: Long = 0
    val r2: Long = 50000
    val r3: Long = 150000
    val r4: Long = matRows
    val c1: Long = r1
    val c2: Long = r2
    val c3: Long = r3
    val c4: Long = matCols

    // init spark context
    val conf = new SparkConf().setAppName("testSvdTaxi")
    val sc = new SparkContext(conf)


    // map lattitude and longitudes to matrix row/column indices
    def coordinatesToMatrixIndices(rdd: RDD[((Double, Double), Int)]): RDD[((Long, Long), Int)] = {
        val rowMin: Double = rdd.map(x => x._1._2).min
        val rowMin2: Double = 40.7687
        val rowMin3: Double = 40.727144
        val rowMax: Double = rdd.map(x => x._1._2).max
        val colMin: Double = rdd.map(x => x._1._1).min
        val colMin2: Double = -74.011147
        val colMin3: Double = -73.968997
        val colMax: Double = rdd.map(x => x._1._1).max
        val mRow1: Double = (r2 - r1).toDouble/(rowMin2 - rowMax)
        val mRow2: Double = (r3 - r2).toDouble/(rowMin3 - rowMin2)
        val mRow3: Double = (r4 - r3).toDouble/(rowMin - rowMin3)
        val mCol1: Double = (c2 - c1).toDouble/(colMin2 - colMin)
        val mCol2: Double = (c3 - c2).toDouble/(colMin3 - colMin2)
        val mCol3: Double = (c4 - c3).toDouble/(colMax - colMin3)

        rdd.map(x => ((colIndex(x._1._1, mCol1, mCol2, mCol3, colMin, colMin2, colMin3), 
                        rowIndex(x._1._2, mRow1, mRow2, mRow3, rowMin2, rowMin3, rowMax)), x._2))
    }

    def rowIndex(in: Double, mRow1: Double, mRow2: Double, mRow3: Double, rowMin2: Double, rowMin3: Double, rowMax: Double): Long = {
        //val rowIdx: Long = 0
        if (in >= rowMin3 && in < rowMax)
        {
            (mRow1 * (in - rowMax) + r1).toLong
        }
        else if (in >= rowMin2 && in < rowMin3)
        {
            (mRow2 * (in - rowMin2) + r2).toLong
        }
        else
        {
            (mRow3 * (in - rowMin3) + r3).toLong
        }
    }

    def colIndex(in: Double, mCol1: Double, mCol2: Double, mCol3: Double, colMin: Double, colMin2: Double, colMin3: Double): Long = {
        // val colIdx: Long = 0
        if (in >= colMin && in < colMin2)
        {
            (mCol1 * (in - colMin) + c1).toLong
        }
        else if (in >= colMin2 && in < colMin3)
        {
            (mCol2 * (in - colMin2) + c2).toLong
        }
        else
        {
            (mCol3 * (in - colMin3) + c3).toLong
        }
    }

    def aggregateRepeatedLocations(rdd: RDD[Array[Double]]): RDD[((Double, Double), Int)] = {
        rdd.map(x => ((x(0), x(1)), 1)).filter(x => x._2 < 575).reduceByKey((a, b) => a + b)
    }

    def main(args: Array[String])
    {
        val t0: Long = System.nanoTime()    // timing variables    
        var t1: Long = 0
        var t2: Long = 0

        // read input data file name
        val file: String = args(0)  
        val numPartitions: Int = args(1).toInt
        val numSingularValues: Int = args(2).toInt
        println("Input file name: " + file)

        // read the data as an RDD
        t1 = System.nanoTime()
        val data1: RDD[Array[Double]] = sc.textFile(file, numPartitions).map(line => line.split(",").map(_.toDouble))
        println("1. RDD num. of Partitions: " + data1.partitions.size)
        //val data: RDD[Array[Double]] = data1.coalesce(numPartitions, false).cache()
        val data: RDD[Array[Double]] = data1.repartition(numPartitions).cache()
        val dataAggregated: RDD[((Double, Double), Int)] = aggregateRepeatedLocations(data)
        val dataWithMatrixIndices: RDD[((Long, Long), Int)] = coordinatesToMatrixIndices(dataAggregated).reduceByKey((a, b) => a + b)
        data.unpersist()
        dataWithMatrixIndices.cache()
        println("2. RDD num. of Partitions: " + dataAggregated.partitions.size)

        // create RowMatrix
        t1 = System.nanoTime()
        val dataCoordMatrix: CoordinateMatrix = new CoordinateMatrix(
                                                    dataWithMatrixIndices.map{case entryVal => 
                                                        new MatrixEntry(entryVal._1._2, entryVal._1._1, entryVal._2.toDouble)}
                                                                    )
        val matData: RowMatrix = dataCoordMatrix.toRowMatrix 
        t2 = System.nanoTime()
        println("Data converted to RowMatrix, time [sec]: " + (t2-t1)/GIGS)
        println("matData dimensions: [" + matData.numRows + " x " + matData.numCols + "]")

        // find svd of matrix matData
        println("Starting SVD...")
        t1 = System.nanoTime()
        val svd: SingularValueDecomposition[RowMatrix, Matrix] = matData.computeSVD(numSingularValues, computeU = true)
        t2 = System.nanoTime()
        println("SVD computed, time [sec]: " + (t2-t1)/GIGS)

        println("Total program run-time [sec]: " + (t2-t0)/GIGS)
    }
}
