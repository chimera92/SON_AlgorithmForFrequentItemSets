import java.io.{File, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Set}
import scala.math.Ordering.Implicits._

/**
  * Created by chimera on 10/13/17.
  */
object Main {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SON").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
//    val caseNo = args(0).toInt

//    val rating_text = sc.textFile(args(2))
    //val user_text = sc.textFile(args(1))
    val caseNo = 1
    val rating_text = sc.textFile("input/ratings.data")
    val user_text = sc.textFile("input/users.data")
    val supportThreshold = 1300

    case class rating(UserID: Int, MovieID: Int, Rating: Int, TimeStamp: Int)
    case class user(UserID: Int, Gender: String, Age: Int, Occupation: String, Zip: String)

    val rating_split = rating_text.map(line => line.split("::")).map(line => (rating(line(0).toInt, line(1).toInt, line(2).toInt, line(3).toInt)))
    val user_split = user_text.map(line => line.split("::")).map(line => (user(line(0).toInt, line(1), line(2).toInt, line(3), line(4))))


    val rating_item = rating_split.map(x => (x.UserID, x))
    val user_item = user_split.map(x => (x.UserID, x))
    val join_table = rating_item.join(user_item)

    var allBuckets : RDD[(Int,List[Int])]= sc.emptyRDD
    if(caseNo==1){
       allBuckets = join_table.filter(row => row._2._2.Gender.toLowerCase == "m").map { x => (x._2._1.UserID, List(x._2._1.MovieID)) }
        .reduceByKey((x1, x2) => x1 ++ x2).partitionBy(new HashPartitioner(Runtime.getRuntime.availableProcessors))
    }
    else if(caseNo==2) {
       allBuckets = join_table.filter(row => row._2._2.Gender.toLowerCase == "f").map { x => (x._2._1.MovieID, List(x._2._1.UserID)) }
        .reduceByKey((x1, x2) => x1 ++ x2).partitionBy(new HashPartitioner(Runtime.getRuntime.availableProcessors))
    }
    val allBasketCount = allBuckets.count() * 1.0


    def findcombination(allBucketSplit: List[(Int, List[Int])], localSupport: Double, combinationsList: List[Int], combinationSize: Int): HashMap[Int, Set[Set[Int]]] = {
      var freqCandidatesFromPartsK = new HashMap[Int, Set[Set[Int]]]

      var combination_count = new HashMap[String, Int]

      var supportPositiveCombinations = Set[Int]()

      val preCandidates = combinationsList.combinations(combinationSize)

      for (combinationList <- preCandidates) {
        val key = combinationList.sorted.mkString("&")
        for (bucket <- allBucketSplit) {
          var exists = true
          for (setEle <- combinationList) {
            exists = exists && bucket._2.contains(setEle)
          }
          if (exists) {
            if (combination_count.contains(key)) {
              combination_count(key) = combination_count(key) + 1
            }
            else {
              combination_count.put(key, 1)
            }
          }
        }
      }


      var returnList = new ListBuffer[String]()
      combination_count.foreach { x =>
        returnList += x._1
        if (x._2 >= localSupport) {
          val kSet = Set() ++ x._1.split("&").map(x => x.toInt)
          if (freqCandidatesFromPartsK.contains(combinationSize)) {
            freqCandidatesFromPartsK(combinationSize) += kSet
          }
          else {
            freqCandidatesFromPartsK.put(combinationSize, Set(kSet))
          }


          kSet.foreach(e => supportPositiveCombinations += e)
        }
      }
      if (supportPositiveCombinations.size > 0) {
        val KminusOneReturnedCAndidates = findcombination(allBucketSplit, localSupport, supportPositiveCombinations.toList, combinationSize + 1)
        for ((k, vSet) <- KminusOneReturnedCAndidates) {
          freqCandidatesFromPartsK.put(k, vSet)
        }
        return freqCandidatesFromPartsK
      }
      else {
        return freqCandidatesFromPartsK
      }

    }


    def Apriori(allBucketSplit: List[(Int, List[Int])], globalCount: Double, globalSupport: Double): Iterator[(Int, mutable.Set[mutable.Set[Int]])] = {

      val noOfSplits = globalCount / allBucketSplit.size
      val localSupport = globalSupport / noOfSplits

      var freqCandidatesFromPartsK = new HashMap[Int, Set[Set[Int]]]()

      var supportPositiveSingletonlistBuffer = new ListBuffer[Int]
      var singletonId_count = new HashMap[Int, Int]
      val singletons = allBucketSplit.flatMap(x => x._2)
      for (movieId <- singletons) {
        if (singletonId_count.contains(movieId)) {
          singletonId_count(movieId) = singletonId_count(movieId) + 1
        }
        else {
          singletonId_count.put(movieId, 1)
        }
      }
      singletonId_count.foreach(x =>
        if (x._2 >= localSupport) {
          supportPositiveSingletonlistBuffer += x._1
          val temp = Set(x._1)
          if (freqCandidatesFromPartsK.contains(1)) {
            freqCandidatesFromPartsK(1) += temp
          }
          else {
            freqCandidatesFromPartsK.put(1, Set(temp))
          }
        }
      )

      if (supportPositiveSingletonlistBuffer.size > 0) {
        val KminusOneReturnedCAndidates = findcombination(allBucketSplit, localSupport, supportPositiveSingletonlistBuffer.toList, 2)
        for ((k, vSet) <- KminusOneReturnedCAndidates) {
          freqCandidatesFromPartsK.put(k, vSet)
        }
      }
      return freqCandidatesFromPartsK.iterator
    }










    val sonCandidates = allBuckets.mapPartitions(x => Apriori(x.toList, allBasketCount, supportThreshold)).reduceByKey((x1,x2)=>x1++x2).collect()

    var son_kSize_candidateList = new HashMap[Int, Set[mutable.Set[Int]]]
    for (elem <- sonCandidates) {
      son_kSize_candidateList.put(elem._1, elem._2)
      var x = new ListBuffer[List[Int]]
      elem._2.foreach(y => x += y.toList.sorted)

    }


    def Son2ndPhase(bucketSplit: List[(Int, List[Int])]): Iterator[(String, Int)] = {
      var combiSetCount = new HashMap[String, Int]

      for (bucket <- bucketSplit) {
        val currBucketSet = bucket._2.toSet
        var i = 1
        while (son_kSize_candidateList.contains(i)) {
          for (candidateSet <- son_kSize_candidateList(i)) {
            if (candidateSet.subsetOf(currBucketSet)) {
              val strSigcandidateSet = candidateSet.toList.sorted.mkString("&")
              if (combiSetCount.contains(strSigcandidateSet)) {
                combiSetCount(strSigcandidateSet) = combiSetCount(strSigcandidateSet) + 1
              }
              else {
                combiSetCount.put(strSigcandidateSet, 1)
              }
            }
          }
          i = i + 1
        }
      }
      return combiSetCount.iterator
    }


    val finalCandidatesBeforePrune = allBuckets.mapPartitions(x => Son2ndPhase(x.toList)).reduceByKey((x1,x2)=>x1+x2).collect()

    var finalCombinations = new HashMap[Int, ListBuffer[List[Int]]]
    for (strSigcandidateSet_count <- finalCandidatesBeforePrune.sorted) {
      if (strSigcandidateSet_count._2 >= supportThreshold) {
        var strSigcandidateSet = strSigcandidateSet_count._1.split("&").map(x => x.toInt).toList.sorted
        if (finalCombinations.contains(strSigcandidateSet.size)) {
          finalCombinations(strSigcandidateSet.size) += strSigcandidateSet
        }
        else {
          finalCombinations.put(strSigcandidateSet.size, ListBuffer(strSigcandidateSet))
        }

      }
    }
    new File("SON.output.txt" ).delete()
    val pw = new PrintWriter(new File("SON.output.txt" ))

    var i = 1
    while (finalCombinations.contains(i)) {
      for (elem <- finalCombinations(i).sorted) {
        pw.print("("+elem.mkString(",")+") ")
      }
      pw.println()
      i = i + 1
    }
    pw.close()

  }
}
