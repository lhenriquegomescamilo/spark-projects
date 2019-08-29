package main.scala
import org.apache.spark.sql.{SparkSession, Row, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.{Days, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{Encoders, SparkSession}
//import org.apache.hadoop.conf.Configuration

object ByTwoOutput {

  def processDayNew(spark: SparkSession,
                    day: String,
                    columns: Seq[String]) = {
    import spark.implicits._

    val data = spark.read
        .format("parquet")
        .load("data/providers/sharethis/processed/day=%s".format(day))
        .withColumn("external_id", concat_ws("|", $"adnxs_id", $"ttd_id", $"mediamath_id", $"lotame_id", $"nlsn_id", $"eyeota_id"))
        .select("estid", "url", "external_id", "country")
    
    val mapper = spark.read
        .format("csv")
        .option("sep", "\t")
        .load("/data/metadata/url_segments.tsv")
        
    val data_domain = data
        .withColumn("parsed", parseURL( $"url" ))
        .withColumn("domain", $"parsed"("_2"))
        .withColumn("extension", $"parsed"("_3"))
        .withColumn("path", $"parsed"("_4"))
        .withColumn("query", $"parsed"("_5") )

    val joint = data_domain.join(mapper, $"domain" === $"_c1" )

    val filter = joint
        .filter(
          ($"_c5" === "*" || $"_c5".isNull || $"_c5" === $"extension") &&
          ($"_c2" === "*" || $"_c2".isNull || $"_c2" === $"path") &&
          (when( $"_c3".isNull, true).otherwise(split($"_c3", "&") === $"query" || $"_c3" === "*"  ))
        )

    val fin = filter
        .select("estid", "country", "external_id", "_c4")
        .withColumn("array", explode(split($"_c4", ",")))
        .groupBy($"estid", $"country", $"external_id")
        .agg(
                concat_ws(",", collect_set($"array")).as("pre_final")
            )
        .withColumn("segments", concat($"pre_final", lit(","), get_country($"country") ))
        .select("estid", "segments", "external_id")
    
    filter.write
        .mode("overwrite")
        .format("csv")
        .option("header", "false")
        .option("sep", " ")
        .save("/data/providers/sharethis/test/day=%s".format(day))

  }

  val domains = List("ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "asia", "at", "au", "aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "biz", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cat", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "com", "coop", "cr", "cu", "cv", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "edu", "ee", "eg", "er", "es", "et", "eu", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gob", "gov", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "info", "int", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jobs", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mg", "mh", "mil", "mk", "ml", "mm", "mn", "mo", "mobi", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "net", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "org", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "pro", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tel", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "xxx", "ye", "yt", "za", "zm", "zw")

  val country_codes = Map("AD" -> 579, "AE" -> 580, "AF" -> 581, "AG" -> 582, "AI" -> 583, "AL" -> 584, "AM" -> 585, "AO" -> 586, "AQ" -> 587, "AR" -> 588, "AS" -> 589, "AT" -> 590, "AU" -> 591, "AW" -> 592, "AX" -> 593, "AZ" -> 594, "BA" -> 595, "BB" -> 596, "BD" -> 597, "BE" -> 598, "BF" -> 599, "BG" -> 600, "BH" -> 601, "BI" -> 602, "BJ" -> 603, "BL" -> 604, "BM" -> 605, "BN" -> 606, "BO" -> 607, "BQ" -> 608, "BR" -> 609, "BS" -> 610, "BT" -> 611, "BV" -> 612, "BW" -> 613, "BY" -> 614, "BZ" -> 615, "CA" -> 616, "CC" -> 617, "CD" -> 618, "CF" -> 619, "CG" -> 620, "CH" -> 621, "CI" -> 622, "CK" -> 623, "CL" -> 624, "CM" -> 625, "CN" -> 626, "CO" -> 627, "CR" -> 628, "CU" -> 629, "CV" -> 630, "CW" -> 631, "CX" -> 632, "CY" -> 633, "CZ" -> 634, "DE" -> 635, "DJ" -> 636, "DK" -> 637, "DM" -> 638, "DO" -> 639, "DZ" -> 640, "EC" -> 641, "EE" -> 642, "EG" -> 643, "EH" -> 644, "ER" -> 645, "ES" -> 646, "ET" -> 647, "FI" -> 648, "FJ" -> 649, "FK" -> 650, "FM" -> 651, "FO" -> 652, "FR" -> 653, "GA" -> 654, "GB" -> 655, "GD" -> 656, "GE" -> 657, "GF" -> 658, "GG" -> 659, "GH" -> 660, "GI" -> 661, "GL" -> 662, "GM" -> 663, "GN" -> 664, "GP" -> 665, "GQ" -> 666, "GR" -> 667, "GS" -> 668, "GT" -> 669, "GU" -> 670, "GW" -> 671, "GY" -> 672, "HK" -> 673, "HM" -> 674, "HN" -> 675, "HR" -> 676, "HT" -> 677, "HU" -> 678, "ID" -> 679, "IE" -> 680, "IL" -> 681, "IM" -> 682, "IN" -> 683, "IO" -> 684, "IQ" -> 685, "IR" -> 686, "IS" -> 687, "IT" -> 688, "JE" -> 689, "JM" -> 690, "JO" -> 691, "JP" -> 692, "KE" -> 693, "KG" -> 694, "KH" -> 695, "KI" -> 696, "KM" -> 697, "KN" -> 698, "KP" -> 699, "KR" -> 700, "KW" -> 701, "KY" -> 702, "KZ" -> 703, "LA" -> 704, "LB" -> 705, "LC" -> 706, "LI" -> 707, "LK" -> 708, "LR" -> 709, "LS" -> 710, "LT" -> 711, "LU" -> 712, "LV" -> 713, "LY" -> 714, "MA" -> 715, "MC" -> 716, "MD" -> 717, "ME" -> 718, "MF" -> 719, "MG" -> 720, "MH" -> 721, "MK" -> 722, "ML" -> 723, "MM" -> 724, "MN" -> 725, "MO" -> 726, "MP" -> 727, "MQ" -> 728, "MR" -> 729, "MS" -> 730, "MT" -> 731, "MU" -> 732, "MV" -> 733, "MW" -> 734, "MX" -> 735, "MY" -> 736, "MZ" -> 737, "NA" -> 738, "NC" -> 739, "NE" -> 740, "NF" -> 741, "NG" -> 742, "NI" -> 743, "NL" -> 744, "NO" -> 745, "NP" -> 746, "NR" -> 747, "NU" -> 748, "NZ" -> 749, "OM" -> 750, "PA" -> 751, "PE" -> 752, "PF" -> 753, "PG" -> 754, "PH" -> 755, "PK" -> 756, "PL" -> 757, "PM" -> 758, "PN" -> 759, "PR" -> 760, "PS" -> 761, "PT" -> 762, "PW" -> 763, "PY" -> 764, "QA" -> 765, "RE" -> 766, "RO" -> 767, "RS" -> 768, "RU" -> 769, "RW" -> 770, "SA" -> 771, "SB" -> 772, "SC" -> 773, "SD" -> 774, "SE" -> 775, "SG" -> 776, "SH" -> 777, "SI" -> 778, "SJ" -> 779, "SK" -> 780, "SL" -> 781, "SM" -> 782, "SN" -> 783, "SO" -> 784, "SR" -> 785, "SS" -> 786, "ST" -> 787, "SV" -> 788, "SX" -> 789, "SY" -> 790, "SZ" -> 791, "TC" -> 792, "TD" -> 793, "TF" -> 794, "TG" -> 795, "TH" -> 796, "TJ" -> 797, "TK" -> 798, "TL" -> 799, "TM" -> 800, "TN" -> 801, "TO" -> 802, "TR" -> 803, "TT" -> 804, "TV" -> 805, "TW" -> 806, "TZ" -> 807, "UA" -> 808, "UG" -> 809, "UM" -> 810, "US" -> 811, "UY" -> 812, "UZ" -> 813, "VA" -> 814, "VC" -> 815, "VE" -> 816, "VG" -> 817, "VI" -> 818, "VN" -> 819, "VU" -> 820, "WF" -> 821, "WS" -> 822, "YE" -> 823, "YT" -> 824, "ZA" -> 825, "ZM" -> 826, "ZW" -> 827)

  def parseURL = udf((url: String) => {
      // First we obtain the query string and the URL divided in /
      val split = url
          .split("\\?")
      val qs = if (split.length > 1) split(1) else ""
      val fields = split(0).split('/')

      // Now we can get the URL path and the section with no path at all
      val path = (if (url.startsWith("http")) fields.slice(3, fields.length)
          else fields.slice(1, fields.length))
      val non_path = (if (url.startsWith("http")) fields(2) else fields(0)).split("\\:")(0)

      // From the non-path, we can get the extension, the domain, and the subdomain
      val parts = non_path.split("\\.").toList
      var extension: ListBuffer[String] = new ListBuffer[String]()
      var count = parts.length
      if (count > 0) {
        var part = parts(count - 1)
        // First we get the extension
        while (domains.contains(part) && count > 1) {
          extension += part
          count = count - 1
          part = parts(count - 1)
        }

        // Now we obtain the domain and subdomain.
        val domain = if (count > 0) parts(count - 1) else ""
        val subdomain = parts.slice(0, count - 1).mkString(".")

        (if (subdomain.startsWith("www.")) subdomain.slice(4, subdomain.length) else subdomain, // Subdomain withou www
          domain, // Domain as is
          extension.toList.reverse.mkString("."), // Extensions as string
          "/" + path.mkString("/"), // path with initial /
          qs.split("&").toList) // List with all the query strings parameters separately
      } else ("", "", "", "", List(""))
  })

  val get_country = udf((key: String) => country_codes.getOrElse(key, 0).toString )

  def download_data(spark: SparkSession, nDays: Int, from: Int, columns: Seq[String]): Unit = {
    // Now we get the list of days to be downloaded
    val format = "yyyyMMdd"
    val end   = DateTime.now.minusDays(from)
    val days = (0 until nDays).map(end.minusDays(_)).map(_.toString(format))

    days.foreach(day => processDayNew(spark, day, columns))
  }

  type OptionMap = Map[Symbol, Int]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--nDays" :: value :: tail =>
        nextOption(map ++ Map('nDays -> value.toInt), tail)
      case "--from" :: value :: tail =>
        nextOption(map ++ Map('from -> value.toInt), tail)
    }
  }

  def main(args: Array[String]) {
    // Parse the parameters
    val options = nextOption(Map(), args.toList)
    val nDays = if (options.contains('nDays)) options('nDays) else 1
    val from = if (options.contains('from)) options('from) else 1

    val spark = SparkSession.builder
        .appName("ShareThis US")
        //.config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")
        .getOrCreate()

    val columns = List("")

    download_data(spark, nDays, from, columns)
  }

}