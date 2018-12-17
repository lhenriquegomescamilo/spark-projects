package main.scala

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object ShareThisUSIngester {
  
  /*
   * This function parses a URL getting the URL path, the URL domain, the URL subdomain,
   * the query string, the extension. Particularly, this function takes the URL, splits
   * it into different parts, and from those parts it gets the following attributes:
   * - subdomain
   * - domain
   * - extension
   * - URL path
   * - query string
   */
  def parseURL(url: String): (String, String, String, String, List[String]) = {
    // First we obtain the query string and the URL divided in /
    val split = url.split("\\?")
    val qs = if (split.length>1) split(1)  else ""
    val fields = split(0).split('/')
    
    // Now we can get the URL path and the section with no path at all
    val path = (if (url.startsWith("http")) fields.slice(3, fields.length) 
                else fields.slice(1, fields.length))
    val non_path = (if (url.startsWith("http")) fields(2) else fields(0)).split("\\:")(0)
    
    // From the non-path, we can get the extension, the domain, and the subdomain
    val parts = non_path.split("\\.").toList
    var extension: ListBuffer[String] = new ListBuffer[String]()
    var count = parts.length
    if (count>0){
        var part = parts(count-1)
        // First we get the extension
        while (domains.contains(part) && count>1) {
            extension += part
            count = count-1
            part = parts(count-1)
        }
        
        // Now we obtain the domain and subdomain.
        val domain = if (count>0) parts(count-1) else ""
        val subdomain = parts.slice(0, count-1).mkString(".")
        
        (if (subdomain.startsWith("www.")) subdomain.slice(4, subdomain.length) else subdomain, // Subdomain withou www
         domain,                                                                                // Domain as is
         extension.toList.reverse.mkString("."),                                                // Extensions as string
         "/"+path.mkString("/"),                                                                // path with initial /
         qs.split("&").toList)                                                                  // List with all the query strings parameters separately
    }
    else ("", "", "", "", List(""))
  }
  
  def parse_line(line: String) = {
      val fields = line.split("\",\"", -1)
      
      val est_id = fields(0).replace("\"", "")
      val url = fields(1).replace("\"", "")
      val parsed = parseURL(url)
      val country = if (fields.length>2) fields(2).replace("\"", "") else "US"
      val country_code = country_codes.getOrElse("US", 0).toString
      val ids = if (fields.length>3) fields(3).replace("\"", "") else ""
      (parsed._2, (est_id, parsed, country_code, ids))
  }
  
  def parse_line_mapper(line:String) = {
      val fields = line.replace("\"", "").split("\t", -1)
      
      val domain = fields(1)
      val path = fields(2)
      val qs = fields(3)
      val segments = fields(4)
      val extension = fields(5)
      val subdomain = fields(6)
      val enabled = fields(7).toInt
      
      (domain, List(((subdomain, extension, path, qs, segments, enabled))))
  }
  
  def map_segments(user_side: (String, (String, String, String, String, List[String]), String, String), 
                   segment_side: (String, String, String, String, String, Int),
                   tokens_b: Broadcast[Map[String, Any]]) : (String, (List[String], String)) = {
      val est_id = user_side._1
      val user_subdomain = user_side._2._1
      val user_extensions = user_side._2._3
      val user_path = user_side._2._4
      val user_qs = user_side._2._5
      val country_code = user_side._3
      val ids = user_side._4
      
      val seg_subdomain = segment_side._1
      val seg_extensions = segment_side._2
      val seg_path = segment_side._3
      val seg_qs = segment_side._4
      val segments = segment_side._5
  
      val matching = (seg_subdomain == "*" || seg_subdomain == "" || user_subdomain.equals(seg_subdomain)) && 
                     (seg_extensions == "*" || seg_extensions == "" || seg_extensions.equals(user_extensions)) &&
                     (seg_path == "*" || seg_path == "" || user_path.contains(seg_path)) &&
                     (seg_qs == "*" || seg_qs == "" || seg_qs.split("&").forall(x => user_qs.contains(x)))
      
      val by_token = tokens_b.value.map(x => if (user_side._2._2.contains(x._1) || user_path.contains(x._1)) x._2.toString else "").filter(x => x.length>0).toList
      var results = ("", (List(""), ""))
      
      if (matching && by_token.length>0) {
          results = (est_id, (segments.split(",").toList ::: List(country_code) ::: by_token, ids))
      } else if (matching) {
          results = (est_id, (segments.split(",").toList ::: List(country_code), ids))
      } else if (by_token.length>0){
          results = (est_id, (by_token ::: List(country_code), ids))
      }
      
      results
  }
  
  
  // First of all we define a list of domains, and the country code associated to every country abbreviation
  val domains = List("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "asia", "at", "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "biz", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cat", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "com", "coop", "cr", "cu", "cv", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "edu", "ee", "eg", "er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gob", "gov", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "info", "int", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jobs", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mil", "mk", "ml", "mm", "mn", "mo", "mobi", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "net", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "org", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "pro", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tel", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "xxx", "ye", "yt", "za", "zm", "zw")
  val country_codes = Map("AD"-> 579, "AE"-> 580, "AF"-> 581, "AG"-> 582, "AI"-> 583, "AL"-> 584, "AM"-> 585, "AO"-> 586, "AQ"-> 587, "AR"-> 588, "AS"-> 589, "AT"-> 590, "AU"-> 591, "AW"-> 592, "AX"-> 593, "AZ"-> 594, "BA"-> 595, "BB"-> 596, "BD"-> 597, "BE"-> 598, "BF"-> 599, "BG"-> 600, "BH"-> 601, "BI"-> 602, "BJ"-> 603, "BL"-> 604, "BM"-> 605, "BN"-> 606, "BO"-> 607, "BQ"-> 608, "BR"-> 609, "BS"-> 610, "BT"-> 611, "BV"-> 612, "BW"-> 613, "BY"-> 614, "BZ"-> 615, "CA"-> 616, "CC"-> 617, "CD"-> 618, "CF"-> 619, "CG"-> 620, "CH"-> 621, "CI"-> 622, "CK"-> 623, "CL"-> 624, "CM"-> 625, "CN"-> 626, "CO"-> 627, "CR"-> 628, "CU"-> 629, "CV"-> 630, "CW"-> 631, "CX"-> 632, "CY"-> 633, "CZ"-> 634, "DE"-> 635, "DJ"-> 636, "DK"-> 637, "DM"-> 638, "DO"-> 639, "DZ"-> 640, "EC"-> 641, "EE"-> 642, "EG"-> 643, "EH"-> 644, "ER"-> 645, "ES"-> 646, "ET"-> 647, "FI"-> 648, "FJ"-> 649, "FK"-> 650, "FM"-> 651, "FO"-> 652, "FR"-> 653, "GA"-> 654, "GB"-> 655, "GD"-> 656, "GE"-> 657, "GF"-> 658, "GG"-> 659, "GH"-> 660, "GI"-> 661, "GL"-> 662, "GM"-> 663, "GN"-> 664, "GP"-> 665, "GQ"-> 666, "GR"-> 667, "GS"-> 668, "GT"-> 669, "GU"-> 670, "GW"-> 671, "GY"-> 672, "HK"-> 673, "HM"-> 674, "HN"-> 675, "HR"-> 676, "HT"-> 677, "HU"-> 678, "ID"-> 679, "IE"-> 680, "IL"-> 681, "IM"-> 682, "IN"-> 683, "IO"-> 684, "IQ"-> 685, "IR"-> 686, "IS"-> 687, "IT"-> 688, "JE"-> 689, "JM"-> 690, "JO"-> 691, "JP"-> 692, "KE"-> 693, "KG"-> 694, "KH"-> 695, "KI"-> 696, "KM"-> 697, "KN"-> 698, "KP"-> 699, "KR"-> 700, "KW"-> 701, "KY"-> 702, "KZ"-> 703, "LA"-> 704, "LB"-> 705, "LC"-> 706, "LI"-> 707, "LK"-> 708, "LR"-> 709, "LS"-> 710, "LT"-> 711, "LU"-> 712, "LV"-> 713, "LY"-> 714, "MA"-> 715, "MC"-> 716, "MD"-> 717, "ME"-> 718, "MF"-> 719, "MG"-> 720, "MH"-> 721, "MK"-> 722, "ML"-> 723, "MM"-> 724, "MN"-> 725, "MO"-> 726, "MP"-> 727, "MQ"-> 728, "MR"-> 729, "MS"-> 730, "MT"-> 731, "MU"-> 732, "MV"-> 733, "MW"-> 734, "MX"-> 735, "MY"-> 736, "MZ"-> 737, "NA"-> 738, "NC"-> 739, "NE"-> 740, "NF"-> 741, "NG"-> 742, "NI"-> 743, "NL"-> 744, "NO"-> 745, "NP"-> 746, "NR"-> 747, "NU"-> 748, "NZ"-> 749, "OM"-> 750, "PA"-> 751, "PE"-> 752, "PF"-> 753, "PG"-> 754, "PH"-> 755, "PK"-> 756, "PL"-> 757, "PM"-> 758, "PN"-> 759, "PR"-> 760, "PS"-> 761, "PT"-> 762, "PW"-> 763, "PY"-> 764, "QA"-> 765, "RE"-> 766, "RO"-> 767, "RS"-> 768, "RU"-> 769, "RW"-> 770, "SA"-> 771, "SB"-> 772, "SC"-> 773, "SD"-> 774, "SE"-> 775, "SG"-> 776, "SH"-> 777, "SI"-> 778, "SJ"-> 779, "SK"-> 780, "SL"-> 781, "SM"-> 782, "SN"-> 783, "SO"-> 784, "SR"-> 785, "SS"-> 786, "ST"-> 787, "SV"-> 788, "SX"-> 789, "SY"-> 790, "SZ"-> 791, "TC"-> 792, "TD"-> 793, "TF"-> 794, "TG"-> 795, "TH"-> 796, "TJ"-> 797, "TK"-> 798, "TL"-> 799, "TM"-> 800, "TN"-> 801, "TO"-> 802, "TR"-> 803, "TT"-> 804, "TV"-> 805, "TW"-> 806, "TZ"-> 807, "UA"-> 808, "UG"-> 809, "UM"-> 810, "US"-> 811, "UY"-> 812, "UZ"-> 813, "VA"-> 814, "VC"-> 815, "VE"-> 816, "VG"-> 817, "VI"-> 818, "VN"-> 819, "VU"-> 820, "WF"-> 821, "WS"-> 822, "YE"-> 823, "YT"-> 824, "ZA"-> 825, "ZM"-> 826, "ZW"-> 827)
  
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("audience generator by keywords").getOrCreate()
    val sc = spark.sparkContext
    
    val path = "/datascience/data_us/ready/"
    val files_ready = FileSystem.get( sc.hadoopConfiguration ).listStatus( new Path(path)).map( x => path+x.getPath.toString.split("/").last).filter(x => !x.contains("_SUCCESS")).toList
    val path_done = "/datascience/data_us/done/"
    val files_done = FileSystem.get( sc.hadoopConfiguration ).listStatus( new Path(path_done)).map( x => path+x.getPath.toString.split("/").last).filter(x => !x.contains("_SUCCESS")).toList
    val files = files_ready diff files_done
    
    val rdds = files.map(x => sc.textFile(x))
    val data_st = sc.union(rdds).map(parse_line)
    
    val rdd_mapper = sc.textFile("/data/metadata/url_segments.tsv")
    val data_mapper = rdd_mapper.map(parse_line_mapper).filter(x => x._2(0)._6>0).reduceByKey((x, y) => x ::: y)
    
    val tokens = Map("econom" -> "32", "finance" -> "32", "finanz" -> "32", "dinero" -> "32", "invest" -> "32", "invertir" -> "32",
                     "bebe" -> "3014", "baby" -> "3014", "embarazo" -> "3013", "pregnant" -> "3013", "family" -> "144", "familia" -> "144", "hijos" -> "3013", "children" -> "3013", "adolescen" -> "3016", "teenager" -> "3016", "maternidad"->"150", "maternity"->"150",
                     "humor" -> "61", "animation" -> "61", "animacion" -> "61", "comedia" -> "61", "comedy" -> "61", "ficcion" -> "131", "fiction" -> "131", "pelicula" -> "92", "movie" -> "92", "music" -> "104", "musica" -> "104", "concierto"->"85", "series"->"131",
                     "turismo"->250, "tourism"->"250", "resort" -> "250", "viaje" -> "250", "vacaciones"-> "250", "vacations"-> "250", "trip"-> "250", "flight"-> "250", "pasajes"-> "250", "journey"-> "250", 
                     "salud"-> "152", "health"-> "152", "adelgaz"-> "154", "diet"-> "154", "dieta"-> "154", "medico"-> "152", 
                     "publicidad"->"26", "advertising"->"26", "business"->"26", "negocio"->"26", "mercado"->"26", "market"->"26", "mba"->"26", 
                     "deporte"->"302", "futbol"->"314", "correr"->"160", "running"->"160", "hiking"->"160", "naturaleza"->"305,247", "golf"->"318", "messi"->"314", "ronaldo"->"314")
                     
    val tokens_b = sc.broadcast(tokens)                 
                     
    val smallLookup = sc.broadcast(data_mapper.collect.toMap)
    val joint = data_st.flatMap { case(key, user_data) =>
      smallLookup.value.get(key).toList.flatMap { segment_data_l =>
        segment_data_l.map( segment_data => map_segments(user_data, segment_data, tokens_b) )
      }
    }
    
    val format = "yyyyMMddHHmm"
    val today = DateTime.now.toString(format)
    if (files.length>0){
        joint.filter(x => x._1.length>0).reduceByKey((x, y) => (x._1 ::: y._1, y._2)).map(x => x._1+" "+x._2._1.distinct.mkString(",")+" "+x._2._2).saveAsTextFile("/datascience/data_us/processed/%s".format(today))
    }
    
    if (files.length>0){
        files.foreach(x => sc.parallelize(List("Done")).saveAsTextFile(x.replace("ready", "done")))
        sc.parallelize(List("Done")).saveAsTextFile("/datascience/data_us/processed/%s.done".format(today))
    }
    
    ////////////////////////////////////////////////
    println(sc.textFile("/datascience/data_us/processed/%s/".format(today)).take(10).foreach(println))
  }
}