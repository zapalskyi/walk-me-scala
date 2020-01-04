package com.walkme.jobs

import java.security.MessageDigest

import com.walkme.jobs.model.model.{Click, Log, View}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

/**
 * @author Serhii Zapalskyi (szap) / WorldTicket A/S
 * @version 03.01.2020
 */
object LogsAnonymizerJob extends App {

  val path = getClass.getResource("/data").getPath
  val outputPath = s"$path/backup/"
  val clicksPath = s"$path/clicks/data.csv"
  val viewsPath = s"$path/views/data.csv"
  val salt = "salt"

  val spark = SparkSession
    .builder
    .appName(getClass.getSimpleName)
    .master("local")
    .getOrCreate()

  import spark.implicits._

  def md5(email: String) = {

    DigestUtils.getMd5Digest
    val md = MessageDigest.getInstance("MD5")
    md.update(email.getBytes())
    md.update(salt.getBytes())
    md.digest().map("%02X" format _).mkString
  }

  def mask(ip: String) = {
    (ip split '.' init) :+ "0" mkString "."
  }

  val veiwsSchema = Encoders.product[View].schema
  val views: Dataset[View] = spark.read
    .option("header", "true")
    .schema(veiwsSchema)
    .csv(viewsPath)
    .as[View]

  val viewLogs: Dataset[Log] = views
    .map(v => Log(v.timestamp, "view", md5(v.email), mask(v.ip)))

  val clickSchema = Encoders.product[Click].schema
  val clicks: Dataset[Click] = spark.read
    .option("header", "true")
    .schema(clickSchema)
    .csv(clicksPath)
    .as[Click]

  val clickLogs: Dataset[Log] = clicks
    .joinWith(views, clicks.col("email") === views.col("email"), "inner")
    .map { case (c, v) => Log(c.timestamp, s"click-${c.element}", md5(c.email), mask(v.ip)) }

  clickLogs.union(viewLogs)
    .repartitionByRange(2, $"timestamp".asc)
    .sortWithinPartitions($"timestamp")
    .write
    .option("header", "true")
    .csv(outputPath)
}
