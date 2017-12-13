package mesosphere.marathon
package metrics.kamon

import akka.actor.{ Actor, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, Props }
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.EntitySnapshot
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.metric.instrument.{ Counter, Histogram }

/**
  * A custom log reporter inspired by [[kamon.logreporter.LogReporter]] that logs Kamon metrics to
  * the logger in a more parsable format.
  */
object LogReporter extends ExtensionId[LogReporterExtension] with ExtensionIdProvider {
  override def lookup(): ExtensionId[_ <: Extension] = LogReporter
  override def createExtension(system: ExtendedActorSystem): LogReporterExtension = new LogReporterExtension(system)
}

class LogReporterExtension(system: ExtendedActorSystem) extends Kamon.Extension with StrictLogging {

  logger.info("Starting the Marathon Kamon LogReporter extension")

  val subscriber = system.actorOf(Props[LogReporterSubscriber], "marathon-log-reporter")

  Kamon.metrics.subscribe("trace", "**", subscriber, permanently = true)
  Kamon.metrics.subscribe("counter", "**", subscriber, permanently = true)
  Kamon.metrics.subscribe("histogram", "**", subscriber, permanently = true)
  Kamon.metrics.subscribe("min-max-counter", "**", subscriber, permanently = true)
  Kamon.metrics.subscribe("gauge", "**", subscriber, permanently = true)
  Kamon.metrics.subscribe("system-metric", "**", subscriber, permanently = true)
}

class LogReporterSubscriber extends Actor with StrictLogging {

  var ticks = 0

  def receive = {
    case tick: TickMetricSnapshot => printMetricSnapshot(tick)
  }

  def printMetricSnapshot(tick: TickMetricSnapshot): Unit = {
    ticks += 1
    logger.debug(s"Received tick $ticks")

    tick.metrics foreach {
      case (entity, snapshot) if entity.category == "counter" =>
        snapshot.counter("counter").foreach(logCounter(entity.name, _))
      case (entity, snapshot) if entity.category == "gauge" =>
        snapshot.gauge("gauge").foreach(logHistogramSimple(entity.name, _))
      case (entity, snapshot) if entity.category == "histogram" =>
        snapshot.histogram("histogram").foreach(logHistogram(entity.name, _))
      case (entity, snapshot) if entity.category == "trace" =>
        logTrace(entity.name, snapshot)
      case (entity, snapshot) if entity.category == "trace-segment" =>
        logger.debug(s"Trace segment ${entity.name}")
      case (entity, _) => logger.debug(s"Ignoring unknown metric ${entity.name} for unknown category ${entity.category}")
    }
  }

  def logCounter(name: String, snapshot: Counter.Snapshot): Unit = {
    logger.debug(s"tick=$ticks $name=${snapshot.count}")
  }

  def logHistogramSimple(name: String, snapshot: Histogram.Snapshot): Unit = {
    logger.debug(s"tick=$ticks $name.min=${snapshot.min} $name.max=${snapshot.max}")
  }

  def logHistogram(name: String, snapshot: Histogram.Snapshot): Unit = {
    logger.debug(s"tick=$ticks $name.min=${snapshot.min} $name.median=${snapshot.percentile(50.0D)} $name.max=${snapshot.max}")
  }

  def logTrace(name: String, snapshot: EntitySnapshot): Unit = {
    snapshot.histogram("elapsed-time").foreach(logHistogram(s"$name.elapsed-nanos", _))
  }
}
