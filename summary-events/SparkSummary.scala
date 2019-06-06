
object SparkSummaryVerification {
  
  def loadCount() {

    val udfToDateUTC = udf((epochMilliUTC: Long) => {
        val dateFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(java.time.ZoneId.of("UTC"))
        dateFormatter.format(java.time.Instant.ofEpochMilli(epochMilliUTC))
      })
    
    val channelDf = spark.read.json("wasbs://telemetry-data-store@ntpproductionall.blob.core.windows.net/derived/wfs/2019-05-{1[3-9]}-*.json.gz")
    val withDateDf = channelDf.withColumn("sync_date", udfToDateUTC(channelDf("syncts")))

    val filterTotalContentAppDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.app" && withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).count.sort(withDateDf("sync_date"))
    println("Total Content Played on App:");
    println(filterTotalContentAppDf.show(30,false));

    val devicesContentAppDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.app"&& withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).agg(countDistinct(withDateDf("dimensions.did"))).sort(withDateDf("sync_date"))
    println("Total Devices that Played Content on App:");
    println(devicesContentAppDf.show(30,false));

    val filterAppSessionsDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.app" && withDateDf("dimensions.type") === "app").groupBy(withDateDf("sync_date")).count.sort(withDateDf("sync_date"))
    println("Total App Sessions:");
    println(filterAppSessionsDf.show(30,false));

    val filterTotalContentPortalDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.portal" && withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).count.sort(withDateDf("sync_date"))
    println("Total Content Plays on Portal:");
    println(filterTotalContentPortalDf.show(30,false));

    val filterDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.app" && withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).agg(sum(withDateDf("edata.eks.time_spent"))/3600).sort(withDateDf("sync_date"))
    println("Total Content Plays on App Time Spent in hours:");
    println(filterDf.show(30,false));

    val filterTotalContentPortalTimeDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.portal" && withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).agg(sum(withDateDf("edata.eks.time_spent"))/3600).sort(withDateDf("sync_date"))
    println("Total Content Play Time on Portal(in hours):");
    println(filterTotalContentPortalTimeDf.show(30,false));

    val filterTotalContent = withDateDf.filter((withDateDf("dimensions.pdata.id") === "prod.diksha.portal" || withDateDf("dimensions.pdata.id") === "prod.diksha.app") && withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).count.sort(withDateDf("sync_date"))
    println("Total Content Plays: ");
    println(filterTotalContent.show(30,false));

    val filterTotalContentTime = withDateDf.filter((withDateDf("dimensions.pdata.id") === "prod.diksha.portal" || withDateDf("dimensions.pdata.id") === "prod.diksha.app") && withDateDf("dimensions.mode") === "play" && withDateDf("dimensions.type") === "content").groupBy(withDateDf("sync_date")).agg(sum(withDateDf("edata.eks.time_spent"))/3600).sort(withDateDf("sync_date"))
    println("Total Content Play Time(in hours):");
    println(filterTotalContentTime.show(30,false));

    val filterAppSessionsTimeDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.app" && withDateDf("dimensions.type") === "app").groupBy(withDateDf("sync_date")).agg(sum(withDateDf("edata.eks.time_spent"))/3600).sort(withDateDf("sync_date"))
    println("Total Time spent on App in hours:");
    println(filterAppSessionsTimeDf.show(30,false));

    val totalAppDevicesDf = withDateDf.filter(withDateDf("dimensions.pdata.id") === "prod.diksha.app").groupBy(withDateDf("sync_date")).agg(countDistinct(withDateDf("dimensions.did"))).sort(withDateDf("sync_date"))
    println("Total Devices On App: ");
    println(totalAppDevicesDf.show(30, false));
  }
}
