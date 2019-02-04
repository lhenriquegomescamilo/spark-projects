from pyspark import SparkConf, SparkContext
from datetime import datetime
from geopy.distance import vincenty
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon


conf = SparkConf().setAppName("matching polygon")
sc = SparkContext(conf=conf)

def process_line_may(s):
    fields = s.split(',')
    if fields[0]!="ad_id":
        return (fields[5], fields[0], float(fields[1]), float(fields[2]))
    return None
    
def process_line(s):
    fields = s.split(',')
    if fields[1]!="ad_id":
        # (timestamp, adid, lat, lng, dev_type)
        return (fields[0], fields[1], float(fields[4]), float(fields[5]), fields[2])


rdd_diciembre = sc.textFile('/data/geo/safegraph/2018/12/*/*.gz').map(process_line)
rdd_enero = sc.textFile('/data/geo/safegraph/2019/01/*/*.gz').map(process_line)
rdd_total = sc.union([rdd_diciembre, rdd_enero])


barrio_chino  =		[[-58.453434,-34.555969],[-58.451336,-34.554692],[-58.449059,-34.557333],[-58.450049,-34.557971],[-58.453434,-34.555969]]



barrio_chino_polygon = Polygon(map(lambda t: (t[1], t[0]), barrio_chino))

# We keep only those events that have occurred in the polygon
rdd_total_chino = rdd_total.filter(lambda r: r is not None and barrio_chino_polygon.contains(Point(r[2], r[3])))   

rdd_total_chino.map(lambda r: r[1]+","+r[4]).saveAsTextFile("hdfs://rely-hdfs/datascience/geo/AR/barrio_chino_60d")