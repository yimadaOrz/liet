from pyspark import SparkContext
sc = SparkContext()

import fiona
import fiona.crs
import shapely
import rtree
import csv
import pyproj
import shapely.geometry as geom
import geopandas as gpd



# We perform the same task using Spark. Here we run the task in
# parallel on each partition (chunk of data). For each task, we
# have to re-create the R-Tree since the index cannot be shared
# across partitions. Note: we have to import the package inside
# processTrips() to respect the closure property.
#
# We also factor out the code for clarity.

def createIndex(geojson):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    '''
    Our aggregation function that iterates through records in each
    partition, checking whether we could find a zone that contain
    the pickup location.
    '''
    import csv
    import pyproj
    import shapely.geometry as geom
    
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    B_index, B_zones = createIndex('boroughs.geojson')    
    N_index, N_zones = createIndex('neigborhood.geojson')    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        if 'NULL'  in row[2:6]:
            continue
        try:
            pickup = geom.Point(proj(float(row[3]), float(row[2])))
            dropoff = geom.Point(proj(float(row[5]), float(row[4])))
            borough = findZone(pickup, B_index, B_zones)
            neighborhood = findNeigborhoods(dropoff, N_index, N_zones)
        except:
            continue
        if borough and neighborhood:
            key = (borough, neighborhood)
            counts[key] = counts.get(key, 0) + 1
    return counts.items()

if __name__ == "__main__":
    

    
    neighborhoods_file = 'neighborhoods.geojson'
    boroughs_file = 'boroughs.geojson'
    
    neighborhoods = gpd.read_file(neighborhoods_file)
    boroughs = gpd.read_file(boroughs_file)

    taxi = sc.textFile(sys.argv[1])

    counts = taxi.filter(lambda row:len(row)>4) \
                 .mapPartitionsWithIndex(processTrips) \
                 .reduceByKey(lambda x,y: x+y) \
                 .map(lambda x: (boroughs.boro_name[x[0][0]],(neighborhoods.neighborhood[x[0][1]],x[1]))) \
                 .groupByKey() \
                 .mapValues(list) \
                 .mapValues(lambda x: sorted(x, key=lambda x:x[1], reverse=True)[:3]) \
                 .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1], x[1][2][0], x[1][2][1]))) \
                 .sortByKey() \
                 .saveAsTextFile(sys.argv[2])
