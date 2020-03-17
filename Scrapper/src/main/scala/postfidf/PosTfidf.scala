package main.scala.postfidf
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.joda.time.{Days, DateTime}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotator.{PerceptronModel, SentenceDetector, Tokenizer, Normalizer}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline

import scala.math.log

import org.apache.commons.lang3.StringUtils


/**
  * The idea of this script is to run random stuff. Most of the times, the idea is
  * to run quick fixes, or tests.
  */
object PosTfidf {


/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR LOADING DATA     //////////////////////
    *
    */
   /**
    * (Method Not used)
   **/   

def getData(
      spark: SparkSession,
      date: String) = {

    val path = "/datascience/scraper/parsed/processed/day=%s".format(date)

    val doc = spark.read
            .format("parquet")
            .option("header", "True")
            .option("sep", "\t")
            .load(path)
            .select("url","text")
            .na.drop()

    doc
  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     PIPELINE FOR POS     //////////////////////
    *
    */
   /**
    * This series of methods build a pipeline for Part of Speech Tagging.
   **/    

val documentAssembler = new DocumentAssembler()               
                        .setInputCol("text")     
                        .setOutputCol("document")     
                        .setCleanupMode("shrink")

val sentenceDetector = new SentenceDetector()
.setInputCols("document")
.setOutputCol("sentence")

val tokenizer = new Tokenizer()
.setInputCols("sentence")
.setOutputCol("token")
.setContextChars(Array("(", ")", "?", "!",":","¡","¿"))
.setTargetPattern("^a-zA-Z0-9")

val spanish_pos = PerceptronModel.load("/datascience/misc/pos_ud_gsd_es_2.4.0_2.4_1581891015986")

val posTagger = spanish_pos
.setInputCols(Array("sentence", "token"))
.setOutputCol("pos")

/**

// LA FORMA QUE NO PUEDO HACER CAMINAR----------

val finisher = new Finisher()
.setInputCols("pos")
.setIncludeMetadata(true)
.setOutputAsArray(true)

val pipeline = new Pipeline().setStages(Array(
    documentAssembler,
    sentenceDetector,
    tokenizer,       
    posTagger,
    finisher
))

var df = pipeline.fit(doc).transform(doc) 

val udfZip = udf((finished_pos: Seq[String], finished_pos_metadata: Seq[(String,String)]) => finished_pos zip finished_pos_metadata)

val udfGet1 = udf((word: Row, index:String ) => word.getAs[String](index))

val udfGet2 = udf((word: Row, index:String ) => word.getAs[Array[String]](index))

df = df.withColumn("zipped",udfZip(col("finished_pos"),col("finished_pos_metadata")))
df.show()
df = df.withColumn("zipped", explode(col("zipped")))
df.show()
df = df.withColumn("tag",udfGet1(col("zipped"),lit("_1")))
df.show()
df = df.filter("tag = 'NOUN' or tag = 'PROPN'")
df.show()

*/


val pipeline = new Pipeline().setStages(Array(
    documentAssembler,
    sentenceDetector,
    tokenizer,
    posTagger
))  

def getWord =
      udf(
        (mapa: Map[String,String]) =>
          mapa("word")
      )

def getString =
udf((array: Seq[String]) => array.map(_.toString).mkString(","))    


def getPOS(docs: DataFrame ): DataFrame = {
    val df_pos = pipeline.fit(docs).transform(docs)
      .withColumn("tmp", explode(col("pos"))).select("url","tmp.*")
      .withColumn("kw", getWord(col("metadata")))
      .select("url","kw","result")
      .filter("result = 'NOUN' or result = 'PROPN'")

    df_pos

  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\     METHODS FOR PARSING KEYWORDS     //////////////////////
    *
    */
   /**
    * This methods clean and parse keywords, removing stopwords, all digits and lowering them.
   **/    



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



def cleanseKws(df_pos: DataFrame ): DataFrame = {
  val stripAccents = udf((kw: String) => StringUtils.stripAccents(kw))
  var df_clean = df_pos
                .withColumn("kw", lower(col("kw")))  
                .withColumn("len",length(col("kw"))) // Filter longitude of words
                .filter("len > 2 and len < 18" )
                .filter("kw is not null")

  df_clean = df_clean.withColumn("kw", stripAccents(col("kw"))) //remove accents ñ's, no se si el tokenizer ya lo hace.
  
  df_clean

  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\    TFIDF METHOD    //////////////////////
    *
    */
   /**
    * This Method calculates tdfidf manually for each keyword.
   **/          

def getTFIDF(df_clean: DataFrame ): DataFrame = {
    val df = df_clean.groupBy("url")
      .agg(collect_list("kw").as("document"))
      .select("url","document")
      .withColumn("doc_id", monotonically_increasing_id())  

    val docCount = df.count().toInt               
            
    val columns = df.columns.map(col) :+
        (explode(col("document")) as "token")
    val unfoldedDocs = df.select(columns: _*)

    //TF: times token appears in document
    val tokensWithTf = unfoldedDocs.groupBy("doc_id", "token")
      .agg(count("document") as "TF")

    //DF: number of documents where a token appears
    val tokensWithDf = unfoldedDocs.groupBy("token")
      .agg(approx_count_distinct(col("doc_id"), 0.02).as("DF"))

    //IDF: logarithm of (Total number of documents divided by DF) . How common/rare a word is.
    def calcIdf =
      udf(
        (docCount: Int,DF: Long) =>
          log(docCount/DF)
      )

    val tokensWithIdf = tokensWithDf.withColumn("IDF", calcIdf(lit(docCount),col("DF")))
    
    //TF-IDF: score of a word in a document.
    //The higher the score, the more relevant that word is in that particular document.
    val tfidf_docs = tokensWithTf
      .join(tokensWithIdf, Seq("token"), "left")
      .withColumn("TFIDF", col("tf") * col("idf"))
      .join(df,Seq("doc_id"),"left")

    tfidf_docs

  }

/**
    *
    *         \\\\\\\\\\\\\\\\\\\\\    MAIN METHOD    //////////////////////
    *
    */
   /**
    * This Method processes text with POS (for Nouns and Proper Nouns) + TFIDF.
   **/          

def processText(db: DataFrame ): DataFrame = {
    
    val docs = db.select("url","text","domain")
                .na.drop()

    val df_pos = getPOS(docs)

    val df_clean = cleanseKws(df_pos)
    df_clean.show()

    val tfidf_docs = getTFIDF(df_clean)

    val df_final = tfidf_docs
    .withColumnRenamed("token","kw")
    .withColumn("stem_kw",lit(""))
    .join(docs,Seq("url"),"left")
    .select("url","domain","kw","stem_kw","TFIDF")
    
    df_final
}  


 /*****************************************************/
  /******************     MAIN     *********************/
  /*****************************************************/
  def main(args: Array[String]) {
    val spark = SparkSession.builder
    .appName("POSTFIDF")
    .config("spark.sql.files.ignoreCorruptFiles", "true")
    .getOrCreate()

    //main method
    //processText(db)



  


  }
}