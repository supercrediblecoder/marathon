package mesosphere.marathon.redux.logic

import java.util.UUID

import org.apache.mesos.Protos.TaskState

import scala.collection.mutable

// Incoming events
trait Event

trait MesosUpdateEvent extends Event {
  def rawTaskId: String
}
case class TaskStateUpdate(state: TaskState, rawTaskId: String) extends MesosUpdateEvent


trait SchedulerEvent
case class StartTask(taskId: String) extends SchedulerEvent

trait ApiEvent
case class StartInstance(appId: String, requestId: String) extends ApiEvent


// Side effects that are executed outside of the loop.
trait Intend
case class FireEvent(event: StartTask) extends Intend

// I don't like that the response handling leaks
case class Complete(respondId: String, instance: Instance) extends Intend
case class Fail(respondId: String) extends Intend
case class Ack() extends Intend

// The main Marathon business logic.
class Marathon {

  // Internal ephemeral state of Marathon.
  val apps: mutable.Map[String, App] = mutable.Map.empty
  val instances: mutable.Map[InstanceId, Instance] = mutable.Map.empty

  /**
    * Main entry point into Marathon's business logic. Everything starts here.
    *
    * @param event
    */
  def update(event: Event): Unit = {
    event match {
      case e: MesosUpdateEvent => update(e)
      case e: ApiEvent => update(e)
    }
  }

  /**
    * Handle Mesos updates for an instance.
    *
    * @param event
    */
  def update(event: MesosUpdateEvent): Unit = {
    val instanceId = InstanceId(event.rawTaskId)
    instances.get(instanceId) match {
      case None => ???
      case Some(instance) =>
        val (maybeNewData, intends) = instance.update(event)

        // Do we have to store the new update?
        maybeNewData.foreach { newInstanceData =>
          instances.update(instanceId, newInstanceData)
          Store.persist(newInstanceData, intends.:+(Ack())) // Probably a Akka Stream source or an Actor?
        }
    }
  }

  /**
    * Handle API events.
    *
    * @param event
    */
  def update(event: ApiEvent): Unit = {
    event match {
      case StartInstance(appId, requestId) =>
        apps.get(appId) match {
          case None => Fail(requestId)
          case Some(app) =>
            val instance = startInstanceFor(app)
            val intends = Seq(
              Complete(requestId, instance)
            )
            Store.persist(instance, intends)
        }
    }
  }

  /**
    * Creates and instance for an app.
    * @param app
    */
  def startInstanceFor(app: App): Instance = {
    val instance = Instance.create()
    // This is a weird way to query the instance id
    instances += instance.taskId.instanceId -> instance
    instance
  }
}

object Store {
  def persist(data: Instance, intends: Seq[Intend]): Unit = {
    // store data.... not versioned yet.
    // evaluate intends
  }
}
