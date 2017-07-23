/*
 * Copyright (c) John R Busch Consulting LLC. All rights reserved.
 * No use, or distribution, of this source code is permitted in any form or
 * means without a valid, written license agreement with John R Busch Consulting LLC
 */

package org.apache.spark.metrics.sink

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.izettle.metrics.influxdb.{InfluxDbHttpSender, InfluxDbReporter, InfluxDbSender}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import scala.collection.JavaConverters._

class InfluxSink(val property: Properties, val registry: MetricRegistry,
                 securityMgr: SecurityManager) extends Sink with Logging {

  val INFLUX_DEFAULT_TIMEOUT = 5000 // milliseconds
  val INFLUX_DEFAULT_PERIOD = 10
  val INFLUX_DEFAULT_UNIT = TimeUnit.SECONDS
  val INFLUX_DEFAULT_PROTOCOL = "https"
  val INFLUX_DEFAULT_PREFIX = ""
  val INFLUX_DEFAULT_TAGS = ""
  val INFLUX_DEFAULT_MEASUREMENTS = ""

  val INFLUX_KEY_PROTOCOL = "protocol"
  val INFLUX_KEY_HOST = "host"
  val INFLUX_KEY_PORT = "port"
  val INFLUX_KEY_PERIOD = "period"
  val INFLUX_KEY_UNIT = "unit"
  val INFLUX_KEY_DATABASE = "database"
  val INFLUX_KEY_AUTH = "auth"
  val INFLUX_KEY_PREFIX = "prefix"
  val INFLUX_KEY_TAGS = "tags"
  val INFLUX_KEY_MEASUREMENTS = "measurements"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (propertyToOption(INFLUX_KEY_HOST).isEmpty) {
    throw new Exception("InfluxDb sink requires 'host' property.")
  }

  if (propertyToOption(INFLUX_KEY_PORT).isEmpty) {
    throw new Exception("InfluxDb sink requires 'port' property.")
  }

  if (propertyToOption(INFLUX_KEY_DATABASE).isEmpty) {
    throw new Exception("InfluxDb sink requires 'database' property.")
  }

  val protocol = propertyToOption(INFLUX_KEY_PROTOCOL).getOrElse(INFLUX_DEFAULT_PROTOCOL)
  val host = propertyToOption(INFLUX_KEY_HOST).get
  val port = propertyToOption(INFLUX_KEY_PORT).get.toInt
  val database = propertyToOption(INFLUX_KEY_DATABASE).get
  val auth = property.getProperty(INFLUX_KEY_AUTH)
  val prefix = propertyToOption(INFLUX_KEY_PREFIX).getOrElse(INFLUX_DEFAULT_PREFIX)
  val tags = propertyToOption(INFLUX_KEY_TAGS).getOrElse(INFLUX_DEFAULT_TAGS)
  val measurements = propertyToOption(INFLUX_KEY_MEASUREMENTS).getOrElse(INFLUX_DEFAULT_MEASUREMENTS)

  val (applicationId, executorId) = {
    val executorRegex = "^\\d+\\.".r
    val metricNames = registry.getNames.asScala
      .filter(name => name != null)

    val appId = metricNames.headOption
      .map(name => name.substring(0, name.indexOf('.'))).getOrElse(Utils.getProcessName())

    val executorId = metricNames
      .map(name => name.substring(name.indexOf('.') + 1))
      .find(name => name.startsWith("driver.") || executorRegex.findPrefixOf(name).isDefined)
      .map { name =>
        val parts = name.split('.')
        parts(0)
      }.getOrElse("unknown")

    (appId, executorId)
  }

  val defaultTags = Seq(
    "host" -> Utils.localHostName(),
    "appId" -> applicationId,
    "executorId" -> executorId)

  // example custom tag input string: "product:my_product,parent:my_service"
  val customTags = tags.split(",")
    .filter(pair => pair.contains(":"))
    .map(pair => (pair.substring(0, pair.indexOf(":")), pair.substring(pair.indexOf(":") + 1, pair.length())))
    .filter { case (k, v) => !k.isEmpty() && !v.isEmpty() }

  val allTags = (defaultTags ++ customTags).toMap

  val pollPeriod: Int = propertyToOption(INFLUX_KEY_PERIOD)
    .map(_.toInt)
    .getOrElse(INFLUX_DEFAULT_PERIOD)

  val pollUnit: TimeUnit = propertyToOption(INFLUX_KEY_UNIT)
    .map(s => TimeUnit.valueOf(s.toUpperCase))
    .getOrElse(INFLUX_DEFAULT_UNIT)

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val measurementMappings: util.LinkedHashMap[String, String] = {
    val _measurementsMap = new util.LinkedHashMap[String, String]()
    measurements.split(",").map(_.trim).foreach { m =>
      _measurementsMap.put(m, ".*\\." + m.replaceAll("\\.", "\\\\."))
    }
    _measurementsMap.put("others", ".*")
    _measurementsMap
  }

  val sender : InfluxDbSender = new InfluxDbHttpSender(protocol, host, port, database, auth,
    TimeUnit.MILLISECONDS, INFLUX_DEFAULT_TIMEOUT, INFLUX_DEFAULT_TIMEOUT, prefix)

  val reporter: InfluxDbReporter = InfluxDbReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .withTags(allTags.asJava)
    .measurementMappings(measurementMappings)
    .groupGauges(true)
    .build(sender)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
