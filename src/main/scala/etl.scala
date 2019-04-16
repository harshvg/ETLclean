
import java.util.Locale
import java.sql.Timestamp

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{broadcast, max, row_number}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.types.{DataTypes, LongType}
import org.apache.spark.sql.functions.struct
import java.sql.Timestamp

import scala.collection.mutable.ListBuffer



object etl {

  def main(args: Array[String]): Unit = {
    //Test()
    //Test
    Test4()

  }

  def Test4(): Unit ={



  }
  def Query3(): Unit ={

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

    // check if an array contains a given word
    val excludeList = Array("I","is")
    def containsWord(word: String): Boolean = {
      for (word2 <- excludeList) {
        if (word2.compareToIgnoreCase(word.toLowerCase(Locale.ENGLISH)) == 0) return true
      }
      false
    }

    // Find all sub words given a big String
    def find_subwords(word :String) : ListBuffer[String]={
      var x = new ListBuffer[String]()
      val pattern = "([a-zA-Z0-9'-]*[a-zA-Z][a-zA-Z0-9-']*)".r
      (pattern findAllIn word).foreach(y=> x+=y)
      x
    }
    val wordsList = find_subwords("Query 3 is su-per-b! I'mmmm lovin' it!")
    for(word <-wordsList ){
      if(!containsWord(word)) println(word)
    }

    def censor(word : String) : String={


    "harsha"
    }


    def calImpactSccore(tweet :String) : Double ={

      0.0
    }

    def generateTopicWords(tweet : String) :Unit={
    }

    def filterShortUrl(tweet: String):Unit={



      "filteredString"
    }


  }

  def ProcezzGzipFiles(): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import org.apache.spark.sql._




    val people = sqlContext.read.json("gs://cmuccpublicdatasets/twitter/s19/")
    people.withColumn("fdsa",to_json(struct("a","b")))


    val z = List("en", "ar", "fr", "in", "pt", "es", "tr", "ja")
    val reviewFiltered = people.filter($"lang" isin (z: _*)).filter($"id_str".isNotNull).filter($"id".isNotNull).filter($"user.id_str".isNotNull).filter($"user.id".isNotNull).filter(($"text" =!= "") or $"text".isNotNull).filter(($"created_at" =!= "") or $"created_at".isNotNull).filter(size($"entities.hashtags") =!= 0).filter($"entities.hashtags".isNotNull).dropDuplicates("id")

    //Tweet table processing
    val tweetDf1 = reviewFiltered.select($"id", $"id_str", $"user.id".alias("user_id"), $"user.id_str".alias("user_id_str"), $"text".alias("content"), $"entities.hashtags.text".alias("hashtags"), $"created_at".alias("created_at2"), $"in_reply_to_user_id".alias("replier_user_id"), $"in_reply_to_user_id_str".alias("replier_id_str"), $"retweeted_status").withColumn("retweet_id_str", when($"retweeted_status".isNotNull, $"retweeted_status.id_str").otherwise(null)).withColumn("org_auth_str", when($"retweeted_status".isNotNull, $"retweeted_status.user.id_str").otherwise(null))
    val tweetDf2 = tweetDf1.withColumn("created_at", to_timestamp($"created_at2", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")).drop("created_at2")

    val finalTweetDf = tweetDf2.withColumn("hashtags", concat_ws(",", $"hashtags"))

    // User table processing
    val user_df = reviewFiltered.select($"user.id_str".alias("user_id_str"), $"user.screen_name".alias("screen_name"), $"user.description".alias("description"), $"id_str".alias("tweet_id_str"), $"created_at".alias("tweet_timestamp"))
    // retweeted original authors
    val retweetDfUsers = reviewFiltered.filter($"retweeted_status".isNotNull).filter($"retweeted_status.user".isNotNull).select($"retweeted_status.user.id_str".alias("user_id_str"), $"retweeted_status.user.screen_name".alias("screen_name"), $"retweeted_status.user.description".alias("description"), $"retweeted_status.id_str".alias("tweet_id_str"), $"created_at".alias("tweet_timestamp"))

    val castedUsers = user_df.withColumn("ts", to_timestamp($"tweet_timestamp", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")).drop("tweet_timestamp")
    val castedretweetusers = retweetDfUsers.withColumn("ts", to_timestamp($"tweet_timestamp", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")).drop("tweet_timestamp")
    castedUsers.printSchema()
    val mergedUsers = castedUsers.union(castedretweetusers)
    val windowtemp = Window.partitionBy("user_id_str").orderBy($"ts".desc)
    val finalUSersDF = mergedUsers.withColumn("rn", row_number.over(windowtemp)).where($"rn" === 1).drop("rn").select($"user_id_str", $"screen_name", $"description", $"tweet_id_str", $"ts")

    finalUSersDF.write.parquet("gs://paraquetstore/cleanmain/FinalUsers.pq")
    finalTweetDf.write.parquet("gs://paraquetstore/cleanmain/FinalTweet.pq")


  }

  def GenerateReplyRetweetsTable(): Unit = {


    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val userDF = spark.read.parquet("gs://paraquetstore/cleanmain/FinalUsers.pq")
    val tweetDF1 = spark.read.parquet("gs://paraquetstore/cleanmain/FinalTweet.pq")


    val tweetDF = tweetDF1.withColumn("casted_id", $"id_str".cast(DataTypes.LongType))


    // 1. Exchange
    val a = tweetDF.filter($"replier_id_str".isNotNull).withColumn("id_3", when($"user_id_str".geq($"replier_id_str"), $"replier_id_str").otherwise($"user_id_str")).withColumn("id_4", when($"replier_id_str".geq($"user_id_str"), $"replier_id_str").otherwise($"user_id_str")).drop("user_id_str", "replier_id_str").withColumnRenamed("id_3", "user_id_str").withColumnRenamed("id_4", "replier_id_str")
    val orderedReplyTweetDF = a.select($"user_id_str".alias("uid"), $"replier_id_str".alias("cid"), $"created_at".alias("ts_tw"), $"content".alias("tweet"), $"hashtags", $"id".alias("tweet_id"), $"casted_id")
    // 2. Get the count
    val replyDF = orderedReplyTweetDF.filter($"cid".isNotNull).select($"uid", $"cid", $"ts_tw", $"tweet", $"tweet_id", $"hashtags").groupBy($"uid", $"cid").count()
    // 3. Get the latest tweet
    val replyWindow = Window.partitionBy($"uid", $"cid").orderBy($"ts_tw".desc, $"casted_id".desc)
    val replyDFlatest = orderedReplyTweetDF.filter($"cid".isNotNull).withColumn("latest", row_number.over(replyWindow)).where($"latest" === 1).drop("latest").select($"uid", $"cid", $"tweet", $"ts_tw", $"tweet_id")
    // 4. Get the whole content
    val replyDFContent = orderedReplyTweetDF.groupBy("uid", "cid").agg(concat_ws(":::", collect_list($"tweet")).alias("tw_list")).select($"uid", $"cid", $"tw_list")

    //5. Get full hastags
    val replyDFhashtags = orderedReplyTweetDF.groupBy("uid", "cid").agg(concat_ws(":::", collect_list($"hashtags")).alias("ht_list")).select($"uid", $"cid", $"ht_list")

    //6. Get the final reply table
    val replyDFFinal = replyDF.join(replyDFlatest, Seq("uid", "cid")).join(replyDFContent, Seq("uid", "cid")).join(replyDFhashtags, Seq("uid", "cid"))


    //retweet interaction table
    // 1. Re order the tweets
    val b = tweetDF.filter($"org_auth_str".isNotNull).withColumn("id_3", when($"user_id_str".geq($"org_auth_str"), $"org_auth_str").otherwise($"user_id_str")).withColumn("id_4", when($"org_auth_str".geq($"user_id_str"), $"org_auth_str").otherwise($"user_id_str")).drop("user_id_str", "org_auth_str").withColumnRenamed("id_3", "user_id_str").withColumnRenamed("id_4", "org_auth_str")
    val orderedRetweetDF = b.select($"user_id_str".alias("uid"), $"org_auth_str".alias("cid"), $"created_at".alias("ts_tw"), $"content".alias("tweet"), $"id".alias("tweet_id"), $"casted_id", $"hashtags")
    //2. Count for interaction score
    val retweetDF = orderedRetweetDF.filter($"cid".isNotNull).select($"uid", $"cid").groupBy($"uid", $"cid").count()

    // 3. Get the latest
    val retweetwindow = Window.partitionBy($"uid", $"cid").orderBy($"ts_tw".desc, $"casted_id".desc)
    val retweetDFLatest = orderedRetweetDF.filter($"cid".isNotNull).withColumn("latest", row_number.over(retweetwindow)).where($"latest" === 1).drop("latest").select($"uid", $"cid", $"tweet", $"ts_tw", $"tweet_id")
    //4.Get the whole content
    val retweetDFContent = orderedRetweetDF.groupBy("uid", "cid").agg(concat_ws(":::", collect_list($"tweet")).alias("tw_list")).select($"uid", $"cid", $"tw_list")
    val retweetDFhashtags = orderedRetweetDF.groupBy("uid", "cid").agg(concat_ws(":::", collect_list($"hashtags")).alias("ht_list")).select($"uid", $"cid", $"ht_list")

    // join the count and latest values
    val retweetDFfinal = retweetDF.join(retweetDFLatest, Seq("uid", "cid")).join(retweetDFContent, Seq("uid", "cid")).join(retweetDFhashtags, Seq("uid", "cid"))
    //retweetDF.show()


    replyDFFinal.write.parquet("gs://paraquetstore/cleanmain/FinalReplyDf.pq")
    retweetDFfinal.write.parquet("gs://paraquetstore/cleanmain/FinalRetweetDf.parquet")
  }

  def GenerateFTtable(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    import java.util.Locale

    val replyDF = spark.read.parquet("gs://paraquetstore/cleanmain/FinalReplyDf.pq")
    val retweetDf = spark.read.parquet("gs://paraquetstore/cleanmain/FinalRetweetDf.parquet")
    val hashtags = spark.read.parquet("gs://paraquetstore/mainserver/Finalhashtagtable.pq")


    def getExcludeList: Array[String] = {

      def get(url: String) = scala.io.Source.fromURL(url).mkString

      val content = get("https://s3.amazonaws.com/cmucc-public/s19/team/popular_hashtags.txt")
      val list: Array[String] = content.split("\\r?\\n")
      return list
    }

    val excludeList = getExcludeList

    def calHashScore(first: java.lang.String, second: java.lang.String): Double = {

      if (first != null && second != null) {

        var htlist1 = first.split(",")
        var htlist2 = second.split(",")
        var freqMap1 = new scala.collection.mutable.HashMap[String, Int]
        var freqMap2 = new scala.collection.mutable.HashMap[String, Int]

        for (word <- htlist1) {
          val word2 = word.toLowerCase(Locale.ENGLISH)
          if (freqMap1.keySet.contains(word2)) {
            freqMap1(word2) = freqMap1(word2) + 1
          } else {
            freqMap1(word2) = 1
          }
        }
        for (word <- htlist2) {
          val word2 = word.toLowerCase(Locale.ENGLISH)
          if (freqMap2.keySet.contains(word2)) {
            freqMap2(word2) = freqMap2(word2) + 1
          } else {
            freqMap2(word2) = 1
          }
        }
        var totalMatches = 0
        for ((word, freq) <- freqMap1) {
          if (freqMap2.keySet.contains(word) && (!containsword(word))) {
            totalMatches = totalMatches + freq + freqMap2(word)
          }
        }
        if (totalMatches > 10) {
          1 + Math.log(1 + totalMatches - 10)
        } else {
          1
        }
      } else {
        0.0
      }
    }

    def containsword(word: String): Boolean = {
      for (word2 <- excludeList) {
        if (word2.compareToIgnoreCase(word.toLowerCase(Locale.ENGLISH)) == 0) return true
      }
      false
    }

    def calInteractionScore(a: java.lang.Integer, b: java.lang.Integer): Double = {

      if (a == null && b != null) {
        Math.log(1 + b)
      }
      if (b == null && a != null) {
        Math.log(1 + (2 * a))
      }
      if (a != null && b != null) {
        Math.log(1 + (2 * a) + b)
      } else return 0.0
    }

    def findOutLatest(reply: Row, retweet: Row): String = {
      //return retweet.getAs[String]("rt_tw")
      if (reply.getAs[Timestamp]("rp_ts").after(retweet.getAs[Timestamp]("rt_ts"))) {
        reply.getAs[String]("rp_tw")
      }
      if (reply.getAs[Timestamp]("rp_ts").before(retweet.getAs[Timestamp]("rt_ts"))) {
        retweet.getAs[String]("rt_tw")
      }

      if (reply.getAs[Long]("rp_id").compareTo(retweet.getAs[Long]("rt_id")) > 0) {
        reply.getAs[String]("rp_tw")
      } else retweet.getAs[String]("rt_tw")

    }


    val calHashtagScore = udf(calHashScore _)
    val interactionScore = udf(calInteractionScore _)
    val absoluteLatest = udf(findOutLatest _)

    //calculate interactionscore and absolute latest
    val join1 =
      replyDF.withColumnRenamed("ts_tw", "rp_ts").withColumnRenamed("tweet", "rp_tw").withColumnRenamed("tweet_id", "rp_id").withColumnRenamed("count", "rp_c").withColumnRenamed("ht_list", "rp_htlist").withColumnRenamed("tw_list", "rp_twlist")
        .join(retweetDf.withColumnRenamed("ts_tw", "rt_ts").withColumnRenamed("tweet_id", "rt_id").withColumnRenamed("count", "rt_c").withColumnRenamed("content", "rt_tw").withColumnRenamed("ht_list", "rt_htlist").withColumnRenamed("tw_list", "rt_twlist").withColumnRenamed("tweet", "rt_tw"), Seq("uid", "cid"), "fullouter")
        //intscore calculation
        .withColumn("intscore", when($"rp_c".isNull, interactionScore(lit(0), $"rt_c")).when($"rt_c".isNull, interactionScore($"rp_c", lit(0))).otherwise(interactionScore($"rp_c", $"rt_c"))).as("d1")
        .join(hashtags.as("ht"), $"d1.uid" === $"ht.user_id_str", "left").withColumnRenamed("ht.hash", "uid_hash").as("d2")
        .join(hashtags.as("ht2"), $"d2.cid" === $"ht2.user_id_str", "left").withColumnRenamed("ht2.hash", "cid_hash").drop("rp_c", "rt_c", "user_id_str").withColumn("abs_latest", when($"rp_tw".isNull, $"rt_tw").when($"rt_tw".isNull, $"rp_tw").otherwise(absoluteLatest(struct("rp_tw", "rp_ts", "rp_id"), struct("rt_tw", "rt_ts", "rt_id")))).select($"uid", $"cid", $"intscore", $"ht2.hash".alias("uid_hash"), $"d2.hash".alias("cid_hash"), $"rp_tw", $"rt_tw", $"abs_latest", $"rp_htlist", $"rt_htlist", $"rt_twlist", $"rp_twlist")

    //calculate hscore
    val join2 = join1.withColumn("hscore", when($"uid_hash".isNull, 1).when($"cid_hash".isNull, 1).otherwise(calHashtagScore($"uid_hash", $"cid_hash")))

    /*
   * encode the relevant columns in the base64 format
   */
    val encoded = join2.withColumn("rp_tw64", base64($"rp_tw")).withColumn("rt_tw64", base64($"rt_tw")).withColumn("abs_latest64", base64($"abs_latest")).withColumn("rp_htlist64", base64($"rp_htlist")).withColumn("rt_htlist64", base64($"rt_htlist")).withColumn("rt_twlist64", base64($"rt_twlist")).withColumn("rp_twlist64", base64($"rp_twlist")).drop("abs_latest", "rp_htlist", "rt_htlist", "rt_twlist", "rp_twlist")


    join2.write.parquet("gs://paraquetstore/cleanmain/FTtableNotEncoded.pq")
  }

  def GenerateHashTagtable(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val tweetdf = spark.read.parquet("/Users/harsha/Desktop/TeamETL/output/FinalTweet.parquet")
    val hashtagtable = tweetdf.select($"user_id_str", $"hashtags")
      .groupBy($"user_id_str")
      .agg(collect_list("hashtags").as("hash"))
      .withColumn("hash", concat_ws(",", $"hash"))
    hashtagtable.show(3, false)
    hashtagtable.write.parquet("/Users/harsha/Desktop/TeamETL/output/Finalhashtagtable.parquet")

  }

  def GenerateUsersTable(): Unit = {

    import org.apache.spark.sql.{Row, SparkSession}
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions.{broadcast, max, row_number}
    import org.apache.spark.sql.expressions.Window
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val people = sqlContext.read.json("gs://paraquetstore/miniserver/*.gz")


    val z = List("en", "ar", "fr", "in", "pt", "es", "tr", "ja")
    val reviewFiltered = people.filter($"lang" isin (z: _*)).filter($"id_str".isNotNull).filter($"id".isNotNull).filter($"user.id_str".isNotNull).filter($"user.id".isNotNull).filter(($"text" =!= "") or $"text".isNotNull).filter(($"created_at" =!= "") or $"created_at".isNotNull).filter(size($"entities.hashtags") =!= 0).filter($"entities.hashtags".isNotNull).dropDuplicates("id")


    // User table processing
    val user_df = reviewFiltered.select($"user.id_str".alias("user_id_str"), $"user.screen_name".alias("screen_name"), $"user.description".alias("description"), $"id_str".alias("tweet_id_str"), $"created_at".alias("tweet_timestamp"))
    // retweeted original authors
    val retweetDfUsers = reviewFiltered.filter($"retweeted_status".isNotNull).filter($"retweeted_status.user".isNotNull).select($"retweeted_status.user.id_str".alias("user_id_str"), $"retweeted_status.user.screen_name".alias("screen_name"), $"retweeted_status.user.description".alias("description"), $"retweeted_status.id_str".alias("tweet_id_str"), $"created_at".alias("tweet_timestamp"))

    val castedUsers = user_df.withColumn("ts", to_timestamp($"tweet_timestamp", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")).drop("tweet_timestamp")
    val castedretweetusers = retweetDfUsers.withColumn("ts", to_timestamp($"tweet_timestamp", "EEE MMM dd HH:mm:ss ZZZZZ yyyy")).drop("tweet_timestamp")
    castedUsers.printSchema()
    val mergedUsers = castedUsers.union(castedretweetusers)
    val windowtemp = Window.partitionBy("user_id_str").orderBy($"ts".desc)
    val finalUSersDF = mergedUsers.withColumn("rn", row_number.over(windowtemp)).where($"rn" === 1).drop("rn").select($"user_id_str", $"screen_name", $"description", $"tweet_id_str", $"ts")

    //finalTweetDf.write.parquet("gs://paraquetstore/mainserver/SecondtryFinalTweet.pq")
    finalUSersDF.write.parquet("gs://paraquetstore/miniserver/FixedUsers.pq")
  }


  def Test2(): Unit ={

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val spark = SparkSession.builder().getOrCreate()

    val x = spark.read.csv("/Users/harsha/Desktop/ETLclean/hbaseschama")
    val y= x.select("_c1").take(1)

    x.show(false)



  }
  def Test(): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val replys = Seq(
      (1, 2, 6, 178, "R,one", "1"),
      (15, 16, 8, 23456789, "R three", "3"),
      (2, 1, 9, 2345678, "R four", "4"),
      (2, 4, 10, 1234567, "R five", "5")
    ).toDF("uid", "cid", "count", "timestamp", "tweet", "tweet_id")

    val retweets = Seq(
      (1, 2, 2, 178, "T,one", "6"),
      (1, 3, 3, 23456789, "T two", "7"),
      (2, 1, 4, 2345678, "T three", "8"),
      (2, 4, 5, 1234567, "T four", "9")
    ).toDF("uid", "cid", "count", "timestamp", "tweet", "tweet_id")



    /*
        val final2 = Seq(
          (1, 2, 2, 178, "T,one", "T2 one", "6"),
          (1, 3, 3, 23456789, "T two", "T2 two", "7"),
          (1, 25, 500, 9, "T 25", "T2 25", "7"),
          (2, 4, 5, 1234567, "T four", "T2 four", "9"),
          (1, 7, 3, 179, "T seven", "T2 seven", "20"),
        ).toDF("uid", "cid", "count", "timestamp", "tweet1", "tweet2", "tweet_id")

        printf("****************************")
        print(util.Properties.versionNumberString)
        final2.show()



        def flatten_col(a :Array[Array[String]]): Array[String] ={
          var toBeret = Array[String]()
          for(b <- a){
            for(c <-b){
              toBeret:+c
            }
          }
          toBeret
        }
        def flatten_h = udf(flatten_col _)

        val groupedByUid=  final2.groupBy($"uid".as("fuid")).agg(concat_ws(",",collect_list(to_json(struct($"cid".as("cid"),$"count".as("count"),$"tweet1".as("reply"),$"tweet2".as("retweet"))))).as("data"))

        groupedByUid.show(false)

        val groupedByCid = final2.groupBy($"cid".as("fuid")).agg(concat_ws(",",collect_list(to_json(struct($"uid".as("cid"),$"count".as("count"),$"tweet1".as("reply"),$"tweet2".as("retweet"))))).as("data"))
        val finalGroupedData = groupedByCid.union(groupedByUid)
        val rt = finalGroupedData.groupBy($"fuid").agg(concat_ws(",",collect_list("data")).as("data")).withColumn("datal",concat_ws(",",$"data".cast("String"))).drop("data")
        groupedByUid.show()
        groupedByCid.show()
        rt.show(10,false)




        val x = Window.partitionBy($"uid").orderBy($"uid")
        val dfTop = final2
          .withColumn("cids", collect_list("cid").over(x))
          .withColumn("tweet1s", collect_list("tweet1").over(x))
          .withColumn("tweet2s", collect_list("tweet2").over(x))
          .withColumn("tss", collect_list("timestamp").over(x))
          .withColumn("counts", collect_list("count").over(x))
          .withColumn("rn", row_number().over(x)).where($"rn" === 1).drop("rn")
          .drop("cid", "timestamp", "tweet1", "tweet2", "count", "tweet_id")
       // dfTop.show(false)

        val y = Window.partitionBy($"cid").orderBy($"cid")
        val dfTop2 = final2
          .withColumn("cids", collect_list("uid").over(y))
          .withColumn("tweet1s", collect_list("tweet1").over(y))
          .withColumn("tweet2s", collect_list("tweet2").over(y))
          .withColumn("tss", collect_list("timestamp").over(y))
          .withColumn("counts", collect_list("count").over(y))
          .drop("uid", "timestamp", "tweet1", "tweet2", "count", "tweet_id")
          .withColumn("rn", row_number().over(y)).where($"rn" === 1).drop("rn")
          .withColumnRenamed("cid", "uid")

        val final3 = dfTop2.union(dfTop)


        val z = Window.partitionBy($"uid").orderBy($"uid")
        val final4 = final3
          .withColumn("cidsfin", flatten(collect_list("cids").over(z)))
          .withColumn("replys", flatten(collect_list("tweet1s").over(z)))
          .withColumn("retweets", flatten(collect_list("tweet2s").over(z)))
          .withColumn("countsfinal", flatten(collect_list("counts").over(z)))
          .withColumn("rn", row_number().over(z)).where($"rn" === 1).drop("rn")
          .drop("cids", "tweet1s", "tweet2s", "counts")

        final4.show(false)
    */
  }




  def GenerateHBaseTable(): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._
    val spark = SparkSession.builder().getOrCreate()

    val ft= spark.read.parquet("gs://paraquetstore/cleanmini2/miniFinalFT.pq")


    val groupByUid= ft.groupBy($"uid".as("fuid")).agg(collect_list(to_json(struct($"cid".as("cid"),$"hscore",$"intscore",$"rp_tw",$"rt_tw",$"abs_latest",$"rp_twlist",$"rt_twlist",$"rp_htlist",$"rt_htlist"))).as("data"))

    val groupByCid=ft.groupBy($"cid".as("fuid")).agg(collect_list(to_json(struct($"uid".as("cid"),$"hscore",$"intscore",$"rp_tw",$"rt_tw",$"abs_latest",$"rp_twlist",$"rt_twlist",$"rp_htlist",$"rt_htlist"))).as("data"))

    val finalData= groupByUid.union(groupByCid)
    val unionFinalData = finalData.groupBy($"fuid".as("uid")).agg(flatten(collect_list($"data".as("data")).as("data")))
  }
}
