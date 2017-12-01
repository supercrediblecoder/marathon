package mesosphere.marathon.redux.logic

import java.util.UUID

import org.apache.mesos.Protos.TaskState

// Target state for instance
object Goal {
  sealed trait EnumVal
  case object Running extends EnumVal
  case object Stopped extends EnumVal
}

case class TaskId(instanceId: InstanceId, incarnation: Int) {
  override def toString: String = s"$instanceId.$incarnation"
}

object TaskId {

  def apply(taskId: String) = {

    val instanceId = InstanceId(taskId)
    val incarnation: Int = {
      val pattern = """^marathon-.*\.(\d+)$""".r
      taskId match {
        case pattern(inc) => inc.toInt
        case _ => throw new IllegalStateException(s"Invalid task id: $taskId")
      }
    }

    TaskId(instanceId, incarnation)
  }
}

case class InstanceId(uuid: UUID) {
  override def toString: String = s"marathon-$uuid"
}

object InstanceId {
  def apply(taskId: String): InstanceId = {
    val uuid: UUID =  {
      val pattern = """^marathon-(.*)\.\d+$""".r
      taskId match {
        case pattern(id) => UUID.fromString(id)
        case _ => throw new IllegalStateException(s"Invalid task id: $taskId")
      }
    }

    InstanceId(uuid)
  }
}

case class Instance(val goalState: Goal.EnumVal, val taskId: TaskId) extends StateMachine[MesosUpdateEvent, Instance] {

  val taskState: Option[TaskState] = Some(TaskState.TASK_STAGING)

  override def update(event: MesosUpdateEvent): (Option[Instance], Seq[Intend]) = {
    event match {
      case TaskStateUpdate(state, _) => update(state)
    }
  }

  /**
    * Handle updates for task state changes.
    *
    * @param state
    * @return
    */
  def update(state: TaskState): (Option[Instance], Seq[Intend]) = {
    state match {
      case TaskState.TASK_FAILED if goalState == Goal.Running => restart()
      case TaskState.TASK_RUNNING if goalState == Goal.Running => noop()
    }
  }

  /**
    * Trigger restart for instance.
    */
  def restart(): (Option[Instance], Seq[Intend]) = {
    val newTaskId = taskId.copy(incarnation = taskId.incarnation + 1)
    val updatedInstance = this.copy(taskId = newTaskId)

    (Some(updatedInstance), Seq(FireEvent(StartTask(newTaskId.toString))))
  }
}

object Instance {
  def create(): Instance = {
    val instanceId = InstanceId(UUID.randomUUID())
    Instance(Goal.Running, TaskId(instanceId, incarnation = 0))
  }
}
