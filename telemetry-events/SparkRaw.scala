
object SparkRawVerification { 

  def loadCount() {
    
    val udfToDateUTC = udf((epochMilliUTC: Long) => {
      val dateFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(java.time.ZoneId.of("UTC"))
      dateFormatter.format(java.time.Instant.ofEpochMilli(epochMilliUTC))
    })

    val channelDf = spark.read.json(s"wasbs://telemetry-data-store@ntpproductionall.blob.core.windows.net/channel/*/raw/2019-05-{0[1-9],1[0-3]}-*.json.gz")
    val withSynctsDf = channelDf.withColumn("sync_date", udfToDateUTC(channelDf("syncts")))

    val searchDf = withSynctsDf.filter(withSynctsDf("eid") === "SEARCH" && withSynctsDf("edata.filters.dialcodes").isNotNull).groupBy(withSynctsDf("sync_date")).count.sort(withSynctsDf("sync_date"))
    println("Total QR Scans: ");
    println(searchDf.show(30, false));

    val contentDownloadsDf = withSynctsDf.filter(withSynctsDf("eid") === "INTERACT" && withSynctsDf("context.pdata.id") === "prod.diksha.app" && withSynctsDf("edata.subtype") === "ContentDownload-Success").groupBy(withSynctsDf("sync_date")).count.sort(withSynctsDf("sync_date"))
    println("Total Content Downloads:");
    println(contentDownloadsDf.show(30, false));
  }
}
