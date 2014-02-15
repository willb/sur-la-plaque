package com.freevariable.surlaplaque.data;

case class ZoneBuckets(z0:Long, z1:Long, z2:Long, z3:Long, z4:Long, z5:Long, z6:Long, z7:Long) {
    def +(other: ZoneBuckets) = new ZoneBuckets(z0+other.z0, z1+other.z1, z2+other.z2, z3+other.z3, z4+other.z4, z5+other.z5, z6+other.z6, z7+other.z7)
    lazy val percentages  = {
        val total = (z0 + z1 + z2 + z3 + z4 + z5 + z6 + z7).toDouble;
        (z0 / total, z1/total, z2/total, z3/total, z4/total, z5/total, z6/total, z7/total)
    }
}

object ZoneBuckets {
    def empty() = new ZoneBuckets(0,0,0,0,0,0,0,0)
}

case class ZoneHistogram(buckets: ZoneBuckets, recorder:((ZoneBuckets, Double, Long) => ZoneBuckets)) {
    def record(sample:Double, ct:Long = 1) = ZoneHistogram(recorder(buckets, sample, ct), recorder)
}

object ZoneHistogram {
    def make(ftp:Int) = {
        val recorder = ((b:ZoneBuckets, sample:Double, ct:Long) => sample match {
            case d:Double if d < 1 => new ZoneBuckets(b.z0+ct,b.z1,b.z2,b.z3,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d > 0 && d <= ftp * 0.55 => new ZoneBuckets(b.z0,b.z1+ct,b.z2,b.z3,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 0.75 => new ZoneBuckets(b.z0,b.z1,b.z2+ct,b.z3,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 0.9 => new ZoneBuckets(b.z0,b.z1,b.z2,b.z3+ct,b.z4,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 1.05 => new ZoneBuckets(b.z0,b.z1,b.z2,b.z3,b.z4+ct,b.z5,b.z6,b.z7)
            case d:Double if d <= ftp * 1.2 => new ZoneBuckets(b.z0,b.z1,b.z2,b.z3,b.z4,b.z5+ct,b.z6,b.z7)
            case d:Double if d <= ftp * 1.5 => new ZoneBuckets(b.z0,b.z1,b.z2,b.z3,b.z4,b.z5,b.z6+ct,b.z7)
            case d:Double if d > ftp * 1.5 => new ZoneBuckets(b.z0,b.z1,b.z2,b.z3,b.z4,b.z5,b.z6,b.z7+ct)
            case _ => b
        }
        )
        new ZoneHistogram(ZoneBuckets.empty(), recorder)
    }
}
