package com.freevariable.surlaplaque.app;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object SLP {
    import java.io.File
    def listFilesInDir(dirname: String): List[String] = {
        val dir = new java.io.File(dirname)
        dir.listFiles.filter(_.isFile).toList.map(dirname + "/" + _.getName.toString).filter(fn => fn.endsWith(".tcx"))
    }
    
    def expandArgs(args: Array[String]): Array[String] = {
        val Pattern = "^-d(.*)$".r
        args.toList.foldLeft(List():List[String])((ls, arg) =>
            arg match {
                case Pattern(dir) => ls ++ listFilesInDir(dir)
                case arg:String => arg::ls
            }
        ).toArray
    }
}

class SLP(sc: SparkContext) {
    import com.freevariable.surlaplaque.importer.extract
    import com.freevariable.surlaplaque.mmp.MMP
    
    def processFiles(files: Array[String]) = 
        sc.parallelize(files.flatMap((s:String) => extract.trackpointDataFromFile(s)))

    def processFiles(files: Array[String], period: Int) = 
        sc.parallelize(files.flatMap((s:String) => MMP.calculate(extract.trackpointDataFromFile(s).toList, period)))
}

trait Common {
    import java.io._
    
    def master = sys.env.get("SLP_MASTER") match {
        case Some(v) => v
        case None => "local[8]"
    }
    
    def appName = "sur-la-plaque"
    
    def ftp = sys.env.get("SLP_FTP") match {
        case Some(v) => v.toInt
        case None => 300
    }
    
    def outputFile = sys.env.get("SLP_OUTPUT_FILE") match {
        case Some("--") => new PrintWriter(System.err)
        case Some(filename) => new PrintWriter(new File(filename))
        case None => new PrintWriter(new File("slp.json"))
    }
    
    def getEnvValue(variable:String, default:String) = sys.env.get(variable) match {
        case Some(v) => v
        case None => default
    }
}
