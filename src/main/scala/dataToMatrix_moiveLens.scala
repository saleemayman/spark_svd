/*
 * This function reads a movie ratings file with 3 columns and converts
 * the data into a Matrix with rows corresponding to users, columns the
 * the movies and the matrix entries the respective ratings.
 * Input file format:
 *      column1 = user IDs
 *      column2 = movie IDs
 *      column3 = ratings
 *
 * Use of this function is highly discouraged for large datasets due to 
 * excessive JVM heap usage. So do not use it!
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import Array._
import java.io._

object movieDataToMatrix
{
    // init spark conctext
    val conf = new SparkConf().setAppName("testSvd1")
    val sc = new SparkContext(conf)

    def main(args: Array[String])
    {
        // read input data file name
        val file: String = args(0)  
        val delimiter: String = args(1) 
        val rowsPerFile: Int = args(2).toInt
        println("Input file name: " + file)

        // read the data as an RDD
        val data: RDD[Array[Double]] = sc.textFile(file).map(line => line.split(delimiter).map(_.toDouble)).unpersist()
        println("2. RDD num. of Partitions: " + data.partitions.size)
        println("Number of entries: " + data.count)

        val maxRows: Int = data.map(x => x(0)).max.toInt
        val numFiles: Int = maxRows/rowsPerFile
        val rem: Int = maxRows%rowsPerFile
        var startRow: Array[Int] = new Array[Int](numFiles)
        var endRow: Array[Int] = new Array[Int](numFiles)

        for (i <- 0 to numFiles-1)
        {
            startRow(i) = i * rowsPerFile
            endRow(i) = (i + 1) * rowsPerFile
        }
        endRow(numFiles-1) = endRow(numFiles-1) + rem

        // divide the RDD into subsets and transform data on the sub data
        for (i <- 0 to numFiles-1)
        {
            val dataSubset: RDD[Array[Double]] = data.filter(x => (x(0)> startRow(i) && x(0) <= endRow(i))).cache
            println("startRow: " + startRow(i) + ", endRow: " + endRow(i))
            println("sub-data size: " + dataSubset.count)
            // dataSubset.cache

            val dataSubset_CM: CoordinateMatrix = new CoordinateMatrix(
                                                    dataSubset.map{case entryVal => 
                                                        new MatrixEntry(entryVal(0).toLong - startRow(i).toLong - 1, entryVal(1).toLong-1, entryVal(2).toDouble)}
                                                                    )
            val nCols = dataSubset_CM.numCols.toInt
            val nRows = dataSubset_CM.numRows.toInt
            println("sub-mat. Rows: " + nRows + ", Cols: " + nCols) 
            
            val dataSubset_LA = dataSubset_CM.toBlockMatrix().toLocalMatrix().transpose.toArray
            var localMatrix = ofDim[Double](nRows, nCols)
            println("local matrix declared.")

            // add the transposed rows to the local matrix
            for (j <- 0 to nRows-1)
            {
                localMatrix(j) = dataSubset_LA.slice(j*nCols, (j+1)*nCols)
            }
            println("local matrix filled.")

            // write data to csv file using java i/o
            var csvFileName: String = "ratings_0" + i.toString
            val csvMat = localMatrix.map{ _.mkString(", ") }.mkString("\n")
            val pw = new PrintWriter(csvFileName)
            val COMMA = ","
            localMatrix.foreach { row => pw.println(row mkString COMMA) }
            pw.flush
            pw.close
            println("File written to disk.")
        }
    }
}
