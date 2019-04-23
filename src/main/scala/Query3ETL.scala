import java.util.Locale

import net.liftweb.json._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, ListBuffer}

object Query3ETL extends App {

   def old(args: Array[String]): Unit = {
    /********** Context **********/

    val conf = new SparkConf()
     conf.setMaster("local")
     conf.setAppName("Word Count")
     val sc = new SparkContext(conf)
     sc.setLogLevel("ERROR")

     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    /*****************************/



    /*********** Imports **********/


    /******************************/


    // get the effective word count
    // get the fav count , retweet count , followers count
    // apply the formula

    import sqlContext.implicits._
    // check if an array contains a given word
    val z = List("en")


     val readd = sqlContext.read.json("/Users/harsha/Desktop/part-r-00000.gz")
     val reviewFiltered = readd.filter($"lang" isin (z: _*)).filter($"id_str".isNotNull).filter($"id".isNotNull).filter($"user.id_str".isNotNull).filter($"user.id".isNotNull).filter(($"text" =!= "") or $"text".isNotNull).filter(($"created_at" =!= "") or $"created_at".isNotNull).dropDuplicates("id")

     val relevantDF = reviewFiltered.select($"favorite_count".alias("fav_cnt"),$"id",$"created_at".alias("ts"),$"user.followers_count".alias("fl_cnt"),$"retweet_count".alias("rt_cnt"),$"text".alias("tweet"))
     val retweetsTs = relevantDF.withColumn("ts2",to_timestamp($"ts","EEE MMM dd HH:mm:ss ZZZZZ yyyy")).withColumn("epochtime",unix_timestamp($"ts2","EEE MMM dd HH:mm:ss ZZZZZ yyyy")).drop("ts2","ts")
     def censorUDF = udf(censorData _)
     def impactUDF = udf(calImpactSccore _)
     def topicUDF  = udf(generateTopicWords _)

     val scoresDf = retweetsTs.withColumn("censored",censorUDF($"tweet"))
       .withColumn("imapctscore",impactUDF($"tweet",$"fav_cnt",$"fl_cnt",$"rt_cnt"))
       .withColumn("freqmap",to_json(topicUDF($"tweet")))
     scoresDf.write.parquet("/Users/harsha/Desktop/ETLclean/data/test.pq")
      sc.stop()
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


  def compareAgaistRef():Boolean={

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val content = get("https://s3.amazonaws.com/cmucc-datasets/15619/s19/query3_ref.txt")
    val lines: Array[String] = content.split("\\r?\\n")
    val r = scala.util.Random
    val ourDf = sqlContext.read.parquet("/Users/harsha/Desktop/ETLclean/data/test.pq")
    case class record(tweet_id:String,created_at:String,user_id:String,censored_text:String,impactScore:String)
    implicit val formats = DefaultFormats
    var i =0
    while(i<10) {
      i=i+1
      val index = r.nextInt(lines.length)
      val jsonObject = parse(lines(index))
      val recordObj = jsonObject.extract[record]
      val tweetId = recordObj.tweet_id.toLong
      val ourImpactScore = ourDf.filter($"id"===tweetId).select("imapctscore").show(false)
      println(recordObj.impactScore)



    }
    sc.stop()
    true

  }

  def generateTopicWords(tweet : String) :HashMap[String,Int]={
    var freqMap: HashMap[String, Int] = HashMap()

    val validWords =getValidWords(tweet)
    validWords.foreach(x=> {
      freqMap.put(x,freqMap.getOrElse(x, 0)+1)
    })
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
