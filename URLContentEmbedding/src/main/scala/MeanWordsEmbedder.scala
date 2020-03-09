package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.WrappedArray

/**
* MeanWordsEmbedder
*
* This class generates a vectorial representation of the scraped content of urls.
* The data is read from html parsed by Newspaper3k python library. It extracts all words from headers and title of html.
* Then, it detects language from stopwords and accent characters.
* And finally, it applies a pretrained word embedding model to each unique word presents in the document
* (this model discarts stopwords). The url representation is the average of all embeddings of unique words.
*
* This version only support spanish language.
*/
object MeanWordsEmbedder {

  // Path of parsed HTMLs to process.
  private val INPUT_PATH = "/datascience/scraper/parsed/processed/"

  // Quality Filter: Minimum number of detected words in HTML by the pretrained embedding model.
  private val MIN_UNIQUE_EMB_WORDS = 3

  // Spanish pretrained embedding model (without stopwords, accents removed, lowered)
  private val SP_EMBEDDING_PATH = "/datascience/scraper/embeddings/models/sp_mean_model_embeddings.csv"

  // List of spanish special characters used in language detection
  private val SP_CHARACTERS = List("á", "é", "í", "ó", "ú","ü", "ñ", "¿", "¡")

  // List of portuguese special characters used in language detection
  private val PT_CHARACTERS = List("á", "â", "ã", "à", "ç", "é", "ê", "í", "ó", "ô", "õ", "ú", "¿", "¡")

  // List of spanish stopwords used in language detection (lowered, accents removed)
  private val SP_STOPWORDS = List("a", "al", "algo", "algunas", "algunos", "ante", "antes", "como", "con", "contra", "cual", "cuando",
                                  "de", "del", "desde", "donde", "durante", "e", "el", "ella", "ellas", "ellos", "en", "entre", "era",
                                  "erais", "eramos", "eran", "eras", "eres", "es", "esa", "esas", "ese", "eso", "esos", "esta", "estaba",
                                  "estabais", "estabamos", "estaban", "estabas", "estad", "estada", "estadas", "estado", "estados", "estais",
                                  "estamos", "estan", "estando", "estar", "estara", "estaran", "estaras", "estare", "estareis", "estaremos",
                                  "estaria", "estariais", "estariamos", "estarian", "estarias", "estas", "este", "esteis", "estemos", "esten",
                                  "estes", "esto", "estos", "estoy", "estuve", "estuviera", "estuvierais", "estuvieramos", "estuvieran",
                                  "estuvieras", "estuvieron", "estuviese", "estuvieseis", "estuviesemos", "estuviesen", "estuvieses",
                                  "estuvimos","estuviste", "estuvisteis", "estuvo", "fue", "fuera", "fuerais", "fueramos", "fueran",
                                  "fueras", "fueron", "fuese", "fueseis", "fuesemos", "fuesen", "fueses", "fui", "fuimos", "fuiste",
                                  "fuisteis", "ha", "habeis", "habia", "habiais", "habiamos", "habian", "habias", "habida", "habidas",
                                  "habido", "habidos", "habiendo", "habra", "habran", "habras", "habre", "habreis", "habremos", "habria",
                                  "habriais", "habriamos", "habrian", "habrias", "han", "has", "hasta", "hay", "haya", "hayais", "hayamos",
                                  "hayan", "hayas", "he", "hemos", "hube", "hubiera", "hubierais", "hubieramos", "hubieran", "hubieras", "hubieron",
                                  "hubiese", "hubieseis", "hubiesemos", "hubiesen", "hubieses", "hubimos", "hubiste", "hubisteis", "hubo",
                                  "la", "las", "le", "les", "lo", "los", "mas", "me", "mi", "mia", "mias", "mio", "mios", "mis", "mucho",
                                  "muchos", "muy", "nada", "ni", "no", "nos", "nosotras", "nosotros", "nuestra", "nuestras", "nuestro", "nuestros",
                                  "o", "os", "otra", "otras", "otro", "otros", "para", "pero", "poco", "por", "porque", "que", "quien", "quienes",
                                  "se", "sea", "seais", "seamos", "sean", "seas", "sentid", "sentida", "sentidas", "sentido", "sentidos", "sera",
                                  "seran", "seras", "sere", "sereis", "seremos", "seria", "seriais", "seriamos", "serian", "serias", "si", "siente",
                                  "sin", "sintiendo", "sobre", "sois", "somos", "son", "soy", "su", "sus", "suya", "suyas", "suyo", "suyos", "tambien",
                                  "tanto", "te", "tendra", "tendran", "tendras", "tendre", "tendreis", "tendremos", "tendria", "tendriais", "tendriamos",
                                  "tendrian", "tendrias", "tened", "teneis", "tenemos", "tenga", "tengais", "tengamos", "tengan", "tengas", "tengo", "tenia",
                                  "teniais", "teniamos", "tenian", "tenias", "tenida", "tenidas", "tenido", "tenidos", "teniendo", "ti", "tiene", "tienen",
                                  "tienes", "todo", "todos", "tu", "tus", "tuve", "tuviera", "tuvierais", "tuvieramos", "tuvieran", "tuvieras", "tuvieron",
                                  "tuviese", "tuvieseis", "tuviesemos", "tuviesen", "tuvieses", "tuvimos", "tuviste", "tuvisteis", "tuvo", "tuya", "tuyas",
                                  "tuyo", "tuyos", "un", "una", "uno", "unos", "vos", "vosotras", "vosotros", "vuestra", "vuestras", "vuestro", "vuestros",
                                  "y", "ya", "yo")

  // List of portuguese stopwords used in language detection (lowered, accents removed)
  private val PT_STOPWORDS = List("a", "ao", "aos", "aquela", "aquelas", "aquele", "aqueles", "aquilo", "as", "ate", "com", "como", "da", "das", "de", "dela", "delas", "dele",
                                  "deles", "depois", "do", "dos", "e", "ela",  "elas", "ele", "eles", "em", "entre", "era", "eram", "eramos", "essa", "essas", "esse", "esses",
                                  "esta", "estamos", "estao", "estas", "estava", "estavam", "estavamos", "este", "esteja", "estejam", "estejamos", "estes", "esteve", "estive",
                                  "estivemos", "estiver", "estivera", "estiveram", "estiveramos", "estiverem", "estivermos", "estivesse", "estivessem", "estivessemos", "estou",
                                  "eu", "foi", "fomos", "for", "fora", "foram", "foramos", "forem", "formos", "fosse", "fossem", "fossemos", "fui", "ha", "haja", "hajam", "hajamos",
                                  "hao", "havemos", "hei", "houve", "houvemos", "houver", "houvera", "houveram", "houveramos", "houverao", "houverei", "houverem", "houveremos",
                                  "houveria", "houveriam", "houveriamos", "houvermos", "houvesse", "houvessem", "houvessemos", "isso", "isto", "ja", "lhe", "lhes", "mais", "mas",
                                  "me", "mesmo", "meu", "meus", "minha", "minhas", "muito", "na", "nao", "nas", "nem", "no", "nos", "nossa", "nossas", "nosso", "nossos", "num",
                                  "numa", "o", "os", "ou", "para", "pela", "pelas", "pelo", "pelos", "por", "qual", "quando", "que", "quem", "sao", "se", "seja", "sejam",
                                  "sejamos", "sem", "sera", "serao", "serei", "seremos", "seria", "seriam", "seriamos", "seu", "seus", "so", "somos", "sou", "sua", "suas",
                                  "tambem", "te", "tem", "temos", "tenha", "tenham", "tenhamos", "tenho", "tera", "terao", "terei", "teremos", "teria", "teriam", "teriamos",
                                  "teu", "teus", "teve", "tinha", "tinham", "tinhamos", "tive", "tivemos", "tiver", "tivera", "tiveram", "tiveramos", "tiverem", "tivermos",
                                  "tivesse", "tivessem", "tivessemos", "tu", "tua", "tuas", "um", "uma", "voce", "voces", "vos")

  // List of english stopwords used in language detection (lowered)
  private val EN_STOPWORDS = List("a", "about", "above", "after", "again", "against", "ain", "all", "am", "an", "and", "any", "are", "aren", "aren't",
                                  "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can", "couldn",
                                  "couldn't", "d", "did", "didn", "didn't", "do", "does", "doesn", "doesn't", "doing", "don", "don't", "down", "during",
                                  "each", "few", "for", "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't", "have", "haven", "haven't",
                                  "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in", "into", "is", "isn",
                                  "isn't", "it", "it's", "its", "itself", "just", "ll", "m", "ma", "me", "mightn", "mightn't", "more", "most", "mustn", "mustn't",
                                  "my", "myself", "needn", "needn't", "no", "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other", "our",
                                  "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn",
                                  "shouldn't", "so", "some", "such", "t", "than", "that", "that'll", "the", "their", "theirs", "them", "themselves", "then", "there",
                                  "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn", "wasn't", "we",
                                  "were", "weren", "weren't", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "won", "won't",
                                  "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
                                  )


  /**
  * It reads content parsed of urls, selects title and headers metadata (description, keywords, og and twitter fields),
  * and generates a text column with all texts concatenated in lowercase.
  *
  * @param nDays
  * @param nHours
  * @param from
  *
  * @return Dataframe <url, domain, content>
  */
  def geatUrlParsedContent(
      spark: SparkSession,
      nDays: Int = -1,
      nHours: Int = -1,
      from: Int = 1) = {
    import spark.implicits._

    // First we obtain the configuration to be allowed to watch if a file exists or not
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)

    // Now we obtain the list of hdfs folders to be read
    val hdfs_files = {
      if (nDays != -1){
          // days resolution
          val endDate = DateTime.now.minusDays(from)
          val days = (0 until nDays.toInt).map(endDate.minusDays(_)).map(_.toString("yyyyMMdd"))
          days
          .map(day => INPUT_PATH + "/day=%s/".format(day))
          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      }
      else{
          // hours resolution
          val endDate = DateTime.now.minusHours(from)
          val hours = (0 until nHours.toInt).map(endDate.minusHours(_)).map( tm => (tm.toString("yyyyMMdd"),tm.toString("hh") ))        

          // Now we obtain the list of hdfs folders to be read
          hours
          .map(dayHourTuple => INPUT_PATH + "/day=%s/hour=%s/".format(dayHourTuple._1, dayHourTuple._2))
          .filter(path => fs.exists(new org.apache.hadoop.fs.Path(path)))
      }
    }
        
  
    val df = spark.read.option("basePath", INPUT_PATH).parquet(hdfs_files: _*)
            .na.fill("")
            .select($"url", $"domain",
                    concat($"title", lit(" "),
                           $"description", lit(" "),
                           $"keywords", lit(" "),
                           $"og_title", lit(" "),
                           $"og_description", lit(" "),
                           $"twitter_title", lit(" "),
                           $"twitter_description").as("content"))
           .withColumn("content", lower($"content"))
    df
  }

  /**
  * Tokenize texts in words without accents.
  *
  * @param df: Dataframe <url, domain, content>
  *
  * @return Dataframe <url, domain, content, words>
  */
  def tokenize(spark: SparkSession,
               dfContent: DataFrame) = {
    import spark.implicits._

    val stripAccents = udf((text: String) => StringUtils.stripAccents(text))

    var df = dfContent.withColumn("tokenizer_input", stripAccents($"content"))


    val regexTokenizer = new RegexTokenizer()
    .setInputCol("tokenizer_input")
    .setOutputCol("words")
    .setPattern("\\W")

    df = regexTokenizer.transform(df).drop("tokenizer_input")
    df
  }

  /**
  * It detects language from stopwords and accent characters.
  * Language support: Spanish - Portuguese - English
  *
  * First it determines the language from number of stopwords in each language.
  * If there is a tie, It determines from accent characters.

  * If there is any stopwords or accent characters assigns null.
  *
  * @param df: Dataframe <url, domain, content, words>
  * @return Dataframe <url, domain, content, words, lang>
  */
  def detectLanguage(spark: SparkSession,
                    dfTokenized: DataFrame) =  { 
   import spark.implicits._
   // it creates local variables (it's needed for udf functions)
   var sp_characters = SP_CHARACTERS
   var pt_characters = PT_CHARACTERS
   var sp_stpowords = SP_STOPWORDS
   var pt_stopwords = PT_STOPWORDS
   var en_stopwords = EN_STOPWORDS

   val nuniqueCharSP = udf((text: String) => sp_characters.map(ch => if(text contains ch) 1 else 0 ).sum )
   val nuniqueCharPT = udf((text: String) => pt_characters.map(ch => if(text contains ch) 1 else 0 ).sum )

    val countStopwordsSP = udf((words: WrappedArray[String]) => words.map(w => if(sp_stpowords contains w ) 1 else 0 ).sum)
    val countStopwordsPT = udf((words: WrappedArray[String]) => words.map(w => if(pt_stopwords contains w ) 1 else 0 ).sum)
    val countStopwordsEN = udf((words: WrappedArray[String]) => words.map(w => if(en_stopwords contains w ) 1 else 0 ).sum)

    val predLanguage = udf((swSP: Int, swPT: Int, swEN: Int, chSP: Int, chPT: Int) => 
      // no stopwords or accents
      if(swSP + swPT + swEN + chSP + chPT == 0) null 
      
      // largest number of stop words 
      else if(swSP > swPT && swSP > swEN ) "sp" 
      else if(swPT > swSP && swPT > swEN ) "pt" 
      else if(swEN > swSP && swEN > swPT ) "en"
    
      // Portuguese - Spanish tie, check accents (default Spanish)
      else if((swSP == swPT && swSP > swEN) && (chPT > chSP)) "pt" 
      else if((swSP == swPT && swSP > swEN) && (chPT <= chSP)) "sp"
    
      // English - Spanish tie, check accents
      else if((swEN == swSP && swEN > swPT) && (chSP > 0)) "sp" 
      else if((swEN == swSP && swEN > swPT) && (chSP == 0)) "en" 
      
      // English - Portuguese tie, check accents
      else if((swEN == swPT && swEN > swSP) && (chPT > 0)) "pt"
      else if((swEN == swPT && swEN > swSP) && (chPT == 0)) "en"
      
      // English - Spanish - Portuguese tie, check accents
      else if(chSP == 0 && chPT == 0) "en" 
      else if(chPT > chSP == 0) "pt" 
      else "sp"
    )

    var df = dfTokenized
      .withColumn("sp_sw_count", countStopwordsSP($"words"))
      .withColumn("pt_sw_count", countStopwordsPT($"words"))
      .withColumn("en_sw_count", countStopwordsEN($"words"))
      .withColumn("sp_char_nunique", nuniqueCharSP($"content"))
      .withColumn("pt_char_nunique", nuniqueCharPT($"content"))
      .withColumn("lang", predLanguage($"sp_sw_count",$"pt_sw_count", $"en_sw_count", $"sp_char_nunique", $"pt_char_nunique" ))
      .drop("sp_sw_count", "pt_sw_count", "en_sw_count", "sp_char_nunique", "pt_char_nunique")

    df
    
  }

  /**
  * It selects urls in Spanish, applies the pretrained word embedding spanish model to each unique word and averages them.
  *
  * Stop words are discarded by the pretrained model.
  *
  * Urls with less than MIN_UNIQUE_EMB_WORDS unique words in the pretrained model are discarded.
  * 
  * @param df: Dataframe <url, domain, content, words, lang>
  * @return Dataframe <url, domain, content, n_words, 1 : 300 >

  */
  def spanishEmbedding(spark: SparkSession,
                       dfTokenized: DataFrame) = {  
    import spark.implicits._
    val wordsEmbeddings = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load(SP_EMBEDDING_PATH)

    val avgColumns = wordsEmbeddings.drop("word").columns.map(name => avg(col(name)).as(name)) 
    var df = dfTokenized
        .filter($"lang" === "sp") 
        .select($"url", $"domain", explode($"words").as("word")).dropDuplicates
        .join(wordsEmbeddings, Seq("word"), "inner")
        .groupBy("url","domain")
        .agg(count(col("word")).as("n_words"), avgColumns: _*)
        .where(col("n_words") >= MIN_UNIQUE_EMB_WORDS)

    df
  }

  /**
  * Process parsed HTMLs and write results.
  */
  def proccess(spark: SparkSession, nDays: Int = -1, nHours: Int = -1, from: Int = 1){

    var df = {
      if(nDays != -1)
        geatUrlParsedContent(spark, nDays = nDays, from = from)
      else
        geatUrlParsedContent(spark, nHours = nHours, from = from)
    }

    df = tokenize(spark, df)
    df = detectLanguage(spark, df)
    
    val dfSPEmbeddings = spanishEmbedding(spark, df)

    val date = DateTime.now().toString("yyyyMMdd")
    val hour = DateTime.now().getHourOfDay()
    dfSPEmbeddings
          .withColumn("day",lit(date))
          .withColumn("hour",lit(hour))
          .withColumn("lang",lit("sp"))
          .orderBy(col("url").asc)
          .write
          .format("parquet")
          .mode("append")
          .partitionBy("lang", "day", "hour")
          .save("/datascience/scraper/embeddings/")

    df
        .withColumn("day",lit(date))
        .withColumn("hour",lit(hour))
        .orderBy(col("url").asc)
        .write
        .format("parquet")
        .mode("append")
        .partitionBy("lang", "day", "hour")
        .save("/datascience/scraper/embeddings/content/")

  }

  type OptionMap = Map[Symbol, Int]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--nHours" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toInt), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else -1
    val nHours = if (options.contains('nHours)) options('nHours) else -1
    val from = if (options.contains('from)) options('from) else 1

    val spark = SparkSession.builder
        .appName("URL Mean Word Embeddings")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    if (nDays != -1)
      proccess(spark, nDays = nDays, from = from)
    else if(nHours != -1)
      proccess(spark, nHours = nHours, from = from)
    else 
      proccess(spark, nDays = 1, from = from)
  }

}
