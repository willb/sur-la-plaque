package com.freevariable.surlaplaque.util;

/** 
    FSD:  flat surface distance.
    
    Object to calculate the distance between two points.  Assumes 
    that the Earth is flat and that you aren't going across the 
    180º meridian or either pole.  In practice, I have few GPS traces
    from any of those locations.
*/
object FSD {
    import math.cos
    import math.sqrt
    import math.pow
    import math.toRadians
    
    // earth's radius in km
    val R: Double = 6371.009
    
    /* calculates the distance between two points, given as lat/lon pairs in degrees */
    def distance(fst:(Double,Double),snd:(Double,Double)): Double = {
        val (lat1,lon1) = fst
        val (lat2,lon2) = snd
        val pt1 = (toRadians(lat1), toRadians(lon1))
        val pt2 = (toRadians(lat2), toRadians(lon2))
        val latDelta = pt1._1 - pt2._1
        val lonDelta = pt1._2 - pt2._2
        val meanLat = toRadians((lat1 + lat2)) / 2
        
        return R * sqrt(pow(lonDelta, 2) + pow(cos(meanLat) * latDelta, 2))
    }
    
    /* calculates the distance between two points, given as lat/lon pairs in degrees */
    def sph_distance(fst:(Double,Double),snd:(Double,Double)): Double = {
        val (lat1,lon1) = fst
        val (lat2,lon2) = snd

        val latDelta = lat2 - lat1
        val lonDelta = lon2 - lon1
        
        val meanLat = toRadians((lat1 + lat2)) / 2

        val K1 = 111.13209 - (0.56605 * cos(2 * meanLat)) + (0.00120 * cos(4 * meanLat))
        val K2 = (111.41513 * cos(meanLat)) - (0.09455 * cos(3 * meanLat)) + (0.00012 * cos(5 * meanLat))
                
        return sqrt(pow(K1 * latDelta, 2) + pow(K2 * lonDelta, 2))
    }
}