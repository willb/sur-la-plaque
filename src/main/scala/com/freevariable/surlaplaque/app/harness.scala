/*
 * This file is a part of the "sur la plaque" toolkit for cycling
 * data analytics and visualization.
 *
 * Copyright (c) 2013--2014 William C. Benton and Red Hat, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.freevariable.surlaplaque.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import com.freevariable.surlaplaque.data.Trackpoint

object ReplHarness extends Common {
    
    def setup(args: Array[String], providedApp: Option[SLP] = None) = {
        // XXX: add optional parameters here to support cluster execution
        val app = providedApp.getOrElse(new SLP(new SparkContext(master, appName)))
        
        app.processFiles(SLP.expandArgs(args))
    }
    
    def mapActivities(args: Array[String]) = {
        val data = setup(args)
        
        val apairs = data.map((tp:Trackpoint) => Pair(tp.activity.getOrElse("UNKNOWN"), Pair(tp.timestamp, tp)))
        
        val activities = apairs.keys.distinct.collect
        
        val pairs = for (activity <- activities) yield {
            val filtered = apairs.filter((tup) => {val (a,_) = tup ; a == activity});
            val timestampedTrackpoints = filtered.map((tup) => {val (_,(t,tp)) = tup; (t, tp.watts)}).sortByKey();
            val samples = timestampedTrackpoints.collect;
            Pair(activity, samples)
        }
        
        pairs.toMap
    }
}
