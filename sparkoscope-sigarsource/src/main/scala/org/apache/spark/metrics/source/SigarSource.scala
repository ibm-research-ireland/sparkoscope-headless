package org.apache.spark.metrics.source

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.hyperic.sigar.Sigar
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Created by Yiannis Gkoufas on 06/08/15.
  */
private[spark] class SigarSource() extends Source {
  override def sourceName: String = "sigar"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val LOGGER = LoggerFactory.getLogger(classOf[SigarSource])
  val sigar = new Sigar

  def pid = sigar.getPid

  register(metricRegistry, new StatefulMetric {
    override val name = "network.sent_per_second"
    override def momentaryValue = networkMetrics().bytesTx
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "network.received_per_second"
    override def momentaryValue = networkMetrics.bytesRx
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "disk.written_per_second"
    override def momentaryValue = diskMetrics.bytesWritten
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "disk.read_per_second"
    override def momentaryValue = diskMetrics.bytesRead
  })

  register(metricRegistry, new Metric[Double] {
    override def name = "cpu.host.count"
    override def value = sigar.getCpuInfoList.length
  })

  // TODO: detailed explanation: http://stackoverflow.com/a/16736599/443366
  val tckConvertsion = 1000f / Process("getconf CLK_TCK").lines.head.toInt

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.sys"
    override def momentaryValue = sigar.getCpu.getSys
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.user"
    override def momentaryValue = sigar.getCpu.getUser
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.wait"
    override def momentaryValue = sigar.getCpu.getWait
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.host.total"
    override def momentaryValue = sigar.getCpu.getTotal
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.process.sys"
    override def momentaryValue = sigar.getProcCpu(pid).getSys
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.process.user"
    override def momentaryValue = sigar.getProcCpu(pid).getUser
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new StatefulMetric {
    override val name = "cpu.process.total"
    override def momentaryValue = sigar.getProcCpu(pid).getTotal
    override val unitConversion = tckConvertsion
  })

  register(metricRegistry, new Metric[Long] {
    override def name = "memory.host.total"
    override def value = sigar.getMem.getTotal
  })

  register(metricRegistry, new Metric[Long] {
    override def name = "memory.host.used"
    override def value = sigar.getMem.getUsed
  })

  register(metricRegistry, new Metric[Long] {
    override def name = "memory.host.free"
    override def value = sigar.getMem.getFree
  })

  register(metricRegistry, new Metric[Long] {
    override def name = "memory.process.total"
    override def value = sigar.getProcMem(pid).getSize
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

      valueDiff / timeWindowInSec / unitConversion
    }

    def momentaryValue: Float

    def unitConversion: Float = 1
  }
}
