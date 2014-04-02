sur-la-plaque
=============

This repository contains some experiments with analytics for cycling data.

See [this blog post](http://chapeau.freevariable.com/2014/04/fitness-data-visualization-with-apache-spark.html) for some background.

To produce the kinds of maps in that post, place a bunch of TCX files with GPS and wattage data in a directory called `activities` and then run `sbt console`.  Once the code is done compiling and you're at the REPL prompt, type this to generate a map:

    com.freevariable.surlaplaque.GPSClusterApp.main(Array("-dactivities"))

By default, the map will go into a file in the current directory called `slp.json`.  Currently all configuration is done through environment variables; some influential ones include:

*  `SLP_MMP_PERIOD` defines the period, in seconds, to calculate mean maximal power for (defaults to 60 seconds)
*  `SLP_CLUSTERS` defines the value of _k_ for k-means clustering (defaults to 128)
*  `SLP_ITERATIONS` defines the iteration count for clustering (defaults to 10)
*  `SLP_MASTER` defines the Spark master to use (defaults to `local[8]`)
*  `SLP_OUTPUT_FILE` defines the name of the output file for GeoJSON data (defaults to `slp.json`)

There are other applications here too but they're subject to change.  More documentation is forthcoming.
