package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.hyperic.sigar.Sigar
import org.slf4j.LoggerFactory

/**
  * Created by Yiannis Gkoufas on 06/08/15.
  */
private[spark] class SigarSource() extends Source {
  override def sourceName: String = "sigar"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val LOGGER = LoggerFactory.getLogger(classOf[SigarSource])
  val sigar = new Sigar()

  register(metricRegistry, new StatefulMetric {
    override val name = "kBytesTxPerSecond"
    override def momentaryValue = networkMetrics().bytesTx / 1024f
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "kBytesRxPerSecond"
    override def momentaryValue = networkMetrics.bytesRx / 1024f
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "kBytesWrittenPerSecond"
    override def momentaryValue = diskMetrics.bytesWritten / 1024f
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "kBytesReadPerSecond"
    override def momentaryValue = diskMetrics.bytesRead / 1024f
  })

  register(metricRegistry, new Metric[Double] {
    override def name: String = "cpu"
    override def value: Double = sigar.getCpuPerc.getCombined * 100f
  })

  register(metricRegistry, new Metric[Double] {
    override def name: String = "ram"
    override def value: Double = sigar.getMem.getUsedPercent
  })

  case class NetworkMetrics(bytesRx: Long, bytesTx: Long)

  case class DiskMetrics(bytesWritten: Long, bytesRead: Long)

  def networkMetrics(): NetworkMetrics = {
    var bytesReceived = 0L
    var bytesTransmitted = 0L

    sigar.getNetInterfaceList.foreach(interface => {
      try
      {
        val netInterfaceStat = sigar.getNetInterfaceStat(interface)
        bytesReceived += netInterfaceStat.getRxBytes
        bytesTransmitted += netInterfaceStat.getTxBytes
      }catch {
        case e: Exception => {
          LOGGER.error("Sigar couldn't get network metrics for interface "+interface+", error: "+e.getMessage)
        }
      }
    })
    NetworkMetrics(bytesReceived, bytesTransmitted)
  }

  def diskMetrics(): DiskMetrics = {
    var bytesWritten = 0L
    var bytesRead = 0L

    sigar.getFileSystemList.foreach(fileSystem => {
      try {
        val diskUsage = sigar.getFileSystemUsage(fileSystem.getDirName)
        val systemBytesWritten = diskUsage.getDiskWriteBytes
        val systemBytesRead = diskUsage.getDiskReadBytes
        if (systemBytesWritten > 0) {
          bytesWritten += systemBytesWritten
        }
        if (systemBytesRead > 0) {
          bytesRead += systemBytesRead
        }
      } catch {
        case e: Exception => {
          LOGGER.error("Sigar couldn't get filesystem usage for filesystem "+fileSystem.getDevName+" mounted at "+fileSystem.getDirName+", error: "+e.getMessage)
        }
      }
    })
    DiskMetrics(bytesWritten, bytesRead)
  }

  def register[T](registry: MetricRegistry, metric: Metric[T]) = {metricRegistry.register(metric.name, metric.gauge)}

  trait Metric[T] {
    def value: T
    def name: String

    def gauge: Gauge[T] = new Gauge[T] {
      override def getValue = value
    }
  }

  trait StatefulMetric extends Metric[Float] {
    var lastProbeTimestamp: Long = System.currentTimeMillis
    var lastValue: Float = momentaryValue

    def value = synchronized {
      val now = System.currentTimeMillis
      val timeWindowInSec = (now - lastProbeTimestamp) / 1000f
      lastProbeTimestamp = now

      val newValue = momentaryValue
      val valueDiff = newValue - lastValue
      lastValue = newValue

      valueDiff / timeWindowInSec
    }

    def momentaryValue: Float
  }
}
