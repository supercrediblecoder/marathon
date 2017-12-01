package mesosphere.marathon.redux.logic

trait StateMachine[Event, Data] {

  def update(event: Event): (Option[Data], Seq[Intend])

  def noop() = (None, Seq.empty[Intend])
}

