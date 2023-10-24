/* Finding Top 10 movies using RDD */

    val t10rdd01 = sc.textFile("")
    t10rdd01.foreach(println)
    val t10rdd02 = t10rdd01.map(x => (x.split("\t")(1).toInt, (x.split("\t")(2).toFloat,1)))
    t10rdd02.take(5)
    val t10rdd03 = t10rdd02.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val t10rddavg = t10rdd03.mapValues{ case (sum, count) =>  sum / count }
    t10rddavg.take(5)
    val t10movierdd01 = sc.textFile("")
    val t10movierdd02 = t10movierdd01.map(x => (x.split(";")(0).toInt, x.split(";")(1).toString))
    t10movierdd02.take(5)
    val joinrdd = t10movierdd02.join(t10rddavg)
    joinrdd.take(5)
    val t10joinrdd02 = joinrdd.map(x => (x._2._1, x._2._2))
    t10joinrdd02.take(5)
    val swaprdd = t10joinrdd02.map( x => (x._2, x._1) )
    val sortedrdd = swaprdd.sortByKey(false)
    val top10movies = sortedrdd.take(10)
    top10movies.foreach(println)

