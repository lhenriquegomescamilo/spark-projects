package main.scala
import main.scala.postfidf.PosTfidf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.Pipeline
import org.joda.time.Days
import org.apache.spark._
import com.johnsnowlabs.nlp.annotators.{Normalizer, Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler,Finisher}
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.WrappedArray
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.{
  upper,
  count,
  col,
  abs,
  udf,
  regexp_replace,
  split,
  lit,
  explode,
  length,
  to_timestamp,
  from_unixtime,
  date_format,
  sum
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{
 StructType,
 StructField,
 StringType,
 IntegerType
}
import org.apache.spark.sql.{Column, Row}
import scala.util.Random.shuffle

/**
  * The idea of this script is to Ingest Urls daily to local servers for Scrapper.
  */
object SelectedKeywords {
  def tokenize( dfContent: DataFrame) = {
    val stripAccents = udf((text: String) => StringUtils.stripAccents(text))

    var df = dfContent.withColumn("tokenizer_input", stripAccents(col("content")))


    val regexTokenizer = new RegexTokenizer()
    .setInputCol("tokenizer_input")
    .setOutputCol("words")
    .setPattern("[^a-zA-Z0-9]")

    df = regexTokenizer.transform(df).drop("tokenizer_input")
    df
  }

  def selected_keywords(spark: SparkSession) {
    // Define udfs to check if all chars are digits
    def isAllDigits(x: String) = x forall Character.isDigit
    val udfDigit = udf((keyword: String) => if (isAllDigits(keyword)) true else false)

    // List of stopwords for spanish, english and portuguese(extraced from nltk library)
    val STOPWORDS = List("a", "al", "algo", "algunas", "algunos", "ante", "antes", "como", "con", "contra", "cual", "cuando",
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
                          "y", "ya", "yo",
                          //Portuguese
                          "a", "ao", "aos", "aquela", "aquelas", "aquele", "aqueles", "aquilo", "as", "ate", "com", "como", "da", "das", "de", "dela", "delas", "dele",
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
                          "tivesse", "tivessem", "tivessemos", "tu", "tua", "tuas", "um", "uma", "voce", "voces", "vos",
                          //English
                          "a", "about", "above", "after", "again", "against", "ain", "all", "am", "an", "and", "any", "are", "aren", "aren't",
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
                          "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves")
    
    val day =  DateTime.now().minusDays(1).toString("yyyyMMdd")
    val today =  DateTime.now().toString("yyyyMMdd")
    val data_parsed = spark.read.format("parquet")
                            .load("/datascience/scraper/parsed/processed/day=%s/".format(day))
                            .na.fill("")
                            .select(col("url"), col("domain"),col("text"),
                                    concat(col("title"), lit(" "),
                                           col("description"), lit(" "),
                                           col("keywords"), lit(" "),
                                           col("og_title"), lit(" "),
                                           col("og_description"), lit(" "),
                                           col("twitter_title"),lit(" "),
                                           col("twitter_description")).as("content")
                                    )
    data_parsed.cache()

    // Tokenize parsed data in list of words
    var document = new DocumentAssembler().setInputCol("content")
                                          .setOutputCol("document")

    val tokenizer = new Tokenizer().setInputCols("document")
                                    .setOutputCol("words")
                                    .setContextChars(Array("(", ")", "?", "!",":","¡","¿"))
                                    .setTargetPattern("^A-Za-z")
                                    //[^a-zA-Z0-9]
                  
    val stemmer = new Stemmer().setInputCols("normalized")
                              .setOutputCol("stem_kw")

    val normalizer = new Normalizer().setInputCols(Array("words"))
                                      .setOutputCol("normalized")
                                      .setLowercase(true)
                                      .setCleanupPatterns(Array("^A-Za-z"))
                              
    val finisher = new Finisher().setInputCols(Array("words","stem_kw"))
                                .setOutputCols(Array("words","stem_kw"))
                                .setOutputAsArray(true)
                           

    val pipeline = new Pipeline().setStages(Array(
        document,
        tokenizer,
        normalizer,
        stemmer,
        finisher
    ))

    val udfZip = udf((words: Seq[String], stemmed: Seq[String]) => words zip stemmed)
    val udfGet = udf((words: Row, index:String ) => words.getAs[String](index))

    var df_article = pipeline.fit(data_parsed).transform(data_parsed)
                      .withColumn("zipped",udfZip(col("words"),col("stem_kw")))
                      .withColumn("zipped", explode(col("zipped")))

    df_article = df_article.withColumn("kw",udfGet(col("zipped"),lit("_1")))
          .withColumn("stem_kw",udfGet(col("zipped"),lit("_2")))
          .withColumn("kw", lower(col("kw")))
          .withColumn("stem_kw", lower(col("stem_kw")))
          .withColumn("TFIDF",lit("0"))

    df_article = df_article.select("url","domain","kw","stem_kw","TFIDF")

    // Get pos tagging with TFIDF from text
    val df_pos = PosTfidf.processText(data_parsed)
           
    // Union both dataframes (selected keywords and pos tagging)
    var df = df_pos.union(df_article)

    df = df.withColumn("len",length(col("kw"))) // Filter longitude of words
            .filter("len > 2 and len < 18" )
            
    df= df.withColumn("digit",udfDigit(col("kw"))) // Filter words that are all digits
          .filter("digit = false")
           
    df = df.filter(!col("kw").isin(STOPWORDS: _*)) // Filter stopwords

    df = df.dropDuplicates()

    // Format fields and save
    df.groupBy("url","domain")
      .agg(collect_list(col("kw")).as("kw"),
            collect_list(col("stem_kw")).as("stem_kw"),
            collect_list(col("TFIDF")).as("TFIDF"))
      .withColumn("kw", concat_ws(" ", col("kw")))
      .withColumn("stem_kw", concat_ws(" ", col("stem_kw")))
      .withColumn("TFIDF", concat_ws(" ", col("TFIDF")))
      .withColumnRenamed("url","url_raw")
      .withColumn("hits",lit(""))
      .withColumn("country",lit(""))
      .select("url_raw","hits","country","kw","TFIDF","domain","stem_kw")
      .withColumn("day",lit(today))
      .repartition(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save("/datascience/scraper/selected_keywords")

    }

  /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(Args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    // First we obtain the Spark session
    val spark = SparkSession.builder
      .appName("Selected Keywords")
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    selected_keywords(spark)

  }
}
