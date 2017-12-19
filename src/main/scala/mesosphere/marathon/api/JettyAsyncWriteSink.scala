package mesosphere.marathon
package api

import akka.stream.stage.{ GraphStageLogic, InHandler }
import akka.stream.{ Attributes, Inlet }
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.util.ByteIterator
import akka.util.ByteIterator.ByteArrayIterator
import akka.{ Done, NotUsed }
import akka.stream.{ Graph, SinkShape }
import akka.util.ByteString
import com.typesafe.scalalogging.StrictLogging
import javax.servlet.{ ServletOutputStream, WriteListener }
import akka.stream.scaladsl._
import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

/**
  * Asynchronous graph stage which writes to an asynchronous ServletOutputStream.
  *
  * Materialized Future will fail if the outputStream is not upgraded to async, or if a writeListener is already
  * registered.
  *
  * Closes the outputStream when the stage completes for any reason (succeeds / fails).
  */
class JettyAsyncWriteSink(outputStream: ServletOutputStream) extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Done]] with StrictLogging {

  val in: Inlet[ByteString] = Inlet("ServletOutputStreamSink")
  override val shape: SinkShape[ByteString] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]
    val logic = new GraphStageLogic(shape) {
      private var flushing = false

      val writePossible = createAsyncCallback[Unit] { _ =>
        flushOrMaybePull()
      }

      val writerFailed = createAsyncCallback[Throwable] { ex =>
        doFail(ex)
      }

      private def doFail(ex: Throwable): Unit = {
        failStage(ex)
        promise.failure(ex)
        Try(outputStream.close())
      }

      override def preStart(): Unit = {
        try {
          outputStream.setWriteListener(new WriteListener {
            override def onWritePossible(): Unit = {
              writePossible.invoke(())
            }

            override def onError(t: Throwable): Unit = {
              logger.error(s"Error in outputStream", t)
              writerFailed.invoke(t)
            }
          })
        } catch {
          case ex: Throwable =>
            doFail(ex)
        }
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          writeLogic(grab(in))
        }

        override def onUpstreamFinish(): Unit = {
          promise.success(Done)
          outputStream.close()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          logger.error("upstream is failed", ex)
          doFail(ex)
        }
      })

      private def maybePull(): Unit =
        if (!isClosed(in)) {
          pull(in)
        }

      /**
        * Set mode to flushing. Returns true if ready for write, false if not.
        */
      private def flushLogic(): Boolean =
        if (outputStream.isReady()) {
          flushing = false
          outputStream.flush()
          outputStream.isReady()
        } else {
          flushing = true
          false
        }

      private def flushOrMaybePull(): Unit = {
        if (!flushing || flushLogic())
          maybePull()
      }

      private def writeLogic(data: ByteString): Unit = {
        require(!flushing, "Error! Should not get here!")
        outputStream.write(data.toArray)
        if (flushLogic()) {
          maybePull()
        } else {
          // The WriteListener will call maybePull()
        }
      }
    }

    (logic, promise.future)
  }
}
