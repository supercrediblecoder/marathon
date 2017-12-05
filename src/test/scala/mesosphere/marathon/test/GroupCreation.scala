package mesosphere.marathon
package test

import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.state._
import com.wix.accord._

trait GroupCreation {
  def createRootGroup(
    apps: Map[AppDefinition.AppKey, AppDefinition] = Group.defaultApps,
    pods: Map[PathId, PodDefinition] = Group.defaultPods,
    groups: Set[Group] = Set.empty,
    dependencies: Set[PathId] = Group.defaultDependencies,
    version: Timestamp = Group.defaultVersion): RootGroup = {
    val group = RootGroup(apps, pods, groups.map(group => group.id -> group)(collection.breakOut), dependencies, version)

    val validation = validate(group)(RootGroup.rootGroupValidator(Set()))
    assert(validation.isSuccess, s"Provided test root group was not valid: ${validation.toString}")

    group
  }

  def createGroup(
    id: PathId,
    apps: Map[AppDefinition.AppKey, AppDefinition] = Group.defaultApps,
    pods: Map[PathId, PodDefinition] = Group.defaultPods,
    groups: Set[Group] = Set.empty,
    dependencies: Set[PathId] = Group.defaultDependencies,
    version: Timestamp = Group.defaultVersion): Group = {
    val groupsById: Map[Group.GroupKey, Group] = groups.map(group => group.id -> group)(collection.breakOut)
    val group = Group(
      id,
      apps,
      pods,
      groupsById,
      dependencies,
      version)

    val validation = validate(group)(Group.validGroup(id.parent, Set()))
    assert(validation.isSuccess, s"Provided test group was not valid: ${validation.toString}")

    group
  }
}
