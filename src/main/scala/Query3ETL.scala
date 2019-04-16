import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Locale

import org.json4s.jackson.Serialization
import java.sql.Timestamp

import scala.collection.mutable.HashMap
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{broadcast, max, row_number}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types.{DataTypes, LongType}
import org.apache.spark.sql.functions.struct
import java.sql.Timestamp

import org.json4s.NoTypeHints

import scala.collection.mutable.ListBuffer

object Query3ETL extends App {

  def old(args: Array[String]): Unit = {
    /********** Context **********/
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val spark = SparkSession.builder().getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    /*****************************/

    /*********** Imports **********/


    /******************************/


    // get the effective word count
    // get the fav count , retweet count , followers count
    // apply the formula

    import sqlContext.implicits._
    // check if an array contains a given word

    val retweets = Seq(
      (1, "Hello Harsha,how are you doing today", 2, 2,5, "T,one",12345),
      (2, "Hey vardhan,what the fuck man,where have you been?", 3, 3, 6,"T two",6789),
      (3, "It is so cool out here", 4, 6,7, "T three",10111213),
      (4, "This place is full of niggers and mexicans", 5, 8,9,14151617)
    ).toDF("tid", "tweet", "fav_cnt", "fl_cnt","rt_cnt", "ts")







  }

  /*
   * In this query, we change the calculation a little bit.
   * Here to calculate the TF-IDF for word w in tweet t, TF = term frequency of
   * word w in t, and IDF = ln(Total number of tweets in range/ Number of
   * tweets with w in it). Also, originally a TF-IDF score only indicates
   * the contribution of a word to a document, now we want you to calculate
   * the aggregate TF-IDF score of a word to all documents (namely all the
   * tweets in the given time range and uid range in the request).
   *
   */

  def generateTopicWords(tweet : String) :HashMap[String,Int]={
    var freqMap: HashMap[String, Int] = HashMap()

    val validWords =getValidWords(tweet)
    validWords.foreach(x=> {
      freqMap.put(x,freqMap.getOrElse(x, 0)+1)
    })
    for(x<-freqMap){
      println(x._2)
    }
    freqMap
  }

  def getValidWords(tweet: String) : ListBuffer[String]={
    val words = getSubwords(tweet)
    val stopWordsLower= getStopWordsLowerCase
    var toRet = new ListBuffer[String]
    for(word <- words){
      if(!stopWordsLower.contains(word.toLowerCase(Locale.ENGLISH))){
        toRet.append(word)
      }

    }
    toRet
  }
  /*
  def containsWord(word: String): Boolean = {
    for (word2 <- excludeList) {
      if (word2.compareToIgnoreCase(word.toLowerCase(Locale.ENGLISH)) == 0) return true
    }
    false
  }
*/

  /*
   * @brief returns list of strings that are considered valid words
   *
   */
  def getSubwords(word :String) : ListBuffer[String]={
    var x = new ListBuffer[String]()
    val pattern = "([a-zA-Z0-9'-]*[a-zA-Z][a-zA-Z0-9-']*)".r
    (pattern findAllIn word).foreach(y=> x+=y)
    x
  }

  /*
   * @brief lower case converted list of stop words
   */
  def getStopWordsLowerCase :ListBuffer[String]={
    val content = get("https://s3.amazonaws.com/cmucc-datasets/15619/s19/stopwords.txt")
    val stopwords: Array[String] = content.split("\\r?\\n")
    val toRet: ListBuffer[String]= new ListBuffer[String]
    stopwords.foreach(x=> toRet.append(x.toLowerCase(Locale.ENGLISH)))
    toRet
  }

  /*
   * @brief Calculates the impact score for a given tweet
   * It can be registerd as UDF function and can be run against the dataframe
   */
  def calImpactSccore(tweet :String,fav_cnt:Int,fl_cnt:Int,rt_cnt:Int) : Double ={
    // Filter the tweet to not to have any short URLS
    val filteredTweet = filterShortUrl(tweet)
    // Get valid  words
    val words = getSubwords(filteredTweet)
    // Get Stop words
    val stopWords = getStopWordsLowerCase
    var validWordCount=0

    for(word <- words){
      if(!stopWords.contains(word.toLowerCase(Locale.ENGLISH))){
        validWordCount+=1
      }
     }
    validWordCount * (fav_cnt+fl_cnt+rt_cnt)
  }

  /*
  *@brief replaces the short url in a string and returns the string
  */
  def filterShortUrl(tweet: String):String={
    val pattern = "(https?|ftp)://[^\\\\t\\\\r\\\\n /$.?#][^\\\\t\\\\r\\\\n ]*".r
    var toRet=tweet
    (pattern findAllIn tweet).foreach(x=> toRet=toRet.replaceAll(x,""))
    toRet
  }

  /*
   * @brief gets the string representation of the rot13 encoded word
   * /TODO mention the references as https://rosettacode.org/wiki/Rot-13#Scala
   *
   */
  def rot13(s: String) = s map {
      case c if 'a' <= c.toLower && c.toLower <= 'm' => c + 13 toChar
      case c if 'n' <= c.toLower && c.toLower <= 'z' => c - 13 toChar
      case c => c
       }

  /*
   * @brief Takes a string and for each group of alphanumeric words it converts
   * to lower case and checks if that word is present in the list of banned words
   * which inturn consists of all lowercase letters , if contains , it replaces
   * everythign between first and last character with *
   */
  def censorData(text : String) : String= {
    val excludeList = getBannedWords
    val pattern = "[a-zA-Z0-9]+".r
    var toRet = text
    var temp = text
    (pattern findAllIn text).foreach(f = x => {
     if(excludeList.contains(x.toLowerCase(Locale.ENGLISH))){
      val length = x.length
      var replacement = ""
      replacement += x.charAt(0)
      var i = 1
      while (i < length - 1) {
        replacement += "*"
        i = i + 1
      }
      if (length - 1 != 0)
        replacement += x.charAt(length - 1)
      toRet = toRet.replaceAll(x, replacement)
    }})
    toRet
  }

  /*
   *@brief gets the Content from the requested URL
   */
  def get(url: String) = {
    scala.io.Source.fromURL(url).mkString
  }

  /*
   * @brief gets the list of banned words , they are converted into
   * their lowercase represenation and appended into list
   */
  def getBannedWords : ListBuffer[String] = {
  val content = get("https://s3.amazonaws.com/cmucc-datasets/15619/s19/go_flux_yourself.txt")
  val bannedWords: Array[String] = content.split("\\r?\\n")
  val toRet: ListBuffer[String]= new ListBuffer[String]
  bannedWords.foreach(x=> toRet.append(rot13(x).toLowerCase(Locale.ENGLISH)))
  toRet
  }
}
