package mesosphere.marathon
package state

import java.util.Objects

import com.wix.accord._
import com.wix.accord.dsl._
import mesosphere.marathon.api.v2.Validation._
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.plugin.{ Group => IGroup }
import mesosphere.marathon.state.Group._
import mesosphere.marathon.state.PathId._

import scala.annotation.tailrec

class Group(
    val id: PathId,
    val apps: Map[AppDefinition.AppKey, AppDefinition] = defaultApps,
    val pods: Map[PathId, PodDefinition] = defaultPods,
    val groupsById: Map[Group.GroupKey, Group] = defaultGroups,
    val dependencies: Set[PathId] = defaultDependencies,
    val version: Timestamp = defaultVersion) extends IGroup {

  def app(appId: PathId): Option[AppDefinition] = {
    group(appId.parent).flatMap(_.apps.get(appId))
  }
  def pod(podId: PathId): Option[PodDefinition] = {
    group(podId.parent).flatMap(_.pods.get(podId))
  }

  def runSpec(id: PathId): Option[RunSpec] = {
    val maybeApp = this.app(id)
    if (maybeApp.isDefined) maybeApp else this.pod(id)
  }

  def exists(id: PathId) = runSpec(id).isDefined

  /**
    * Searches for group with id gid in currentGroup.
    *
    * @param currentGroup The group that is searched.
    * @param gid The id of the group that we wish to find.
    * @param remainingParents The parents that we did not walk yet.
    * @return None if no group was found or the group with id gid.
    */
  @tailrec final def group(currentGroup: Group, gid: PathId, remainingParents: List[PathId]): Option[Group] = {
    if (currentGroup.id == gid) Some(currentGroup)
    else {
      // Let's say we look for /test/foo/bar and the consumed path is /test then the next child is /test/foo
      //val nextChild = consumedPath.append(gid.restOf(consumedPath).rootPath)
      currentGroup.groupsById.get(remainingParents.head) match {
        case None => None
        case Some(childGroup) => group(childGroup, gid, remainingParents.tail)
      }
    }
  }

  /**
    * Find and return the child group for the given path. If no match is found, then returns None
    *
    * @param gid The child group to find.
    * @return None if no group was found or the group.
    */
  def group(gid: PathId): Option[Group] = {
    transitiveGroupsById.get(gid)
    //if (id == gid) return Some(this)
    //else group(this, gid, gid.allParents.reverse.tail.:+(gid))
  }

  def transitiveApps: Iterator[AppDefinition] = apps.valuesIterator ++ groupsById.valuesIterator.flatMap(_.transitiveApps)
  def transitiveAppIds: Iterator[PathId] = apps.keysIterator ++ groupsById.valuesIterator.flatMap(_.transitiveAppIds)
  //  def transitiveAppsIterator: Iterator[AppDefinition] = apps.valuesIterator ++ groupsById.valuesIterator.flatMap(_.transitiveAppsIterator)
  //  lazy val transitiveApps: Iterable[AppDefinition] = new Iterable[AppDefinition] {
  //    override def iterator: Iterator[AppDefinition] = transitiveAppsIterator
  //  }
  //  def transitiveAppIdsIterator: Iterator[PathId] = apps.keysIterator ++ groupsById.valuesIterator.flatMap(_.transitiveAppIdsIterator)
  //  lazy val transitiveAppIds: Iterable[PathId] = new Iterable[PathId] {
  //    override def iterator: Iterator[GroupKey] = transitiveAppIdsIterator
  //  }

  def transitivePods: Iterator[PodDefinition] = pods.valuesIterator ++ groupsById.valuesIterator.flatMap(_.transitivePods)
  def transitivePodIds: Iterator[PathId] = pods.keysIterator ++ groupsById.valuesIterator.flatMap(_.transitivePodIds)
  //  def transitivePodsIterator: Iterator[PodDefinition] = pods.valuesIterator ++ groupsById.valuesIterator.flatMap(_.transitivePodsIterator)
  //  lazy val transitivePods: Iterable[PodDefinition] = transitivePodsIterator.toIterable
  //  def transitivePodIdsIterator: Iterator[PathId] = pods.keysIterator ++ groupsById.valuesIterator.flatMap(_.transitivePodIdsIterator)
  //  lazy val transitivePodIds: Iterable[PathId] = transitivePodIdsIterator.toIterable

  def transitiveRunSpecs: Iterator[RunSpec] = transitiveApps ++ transitivePods
  def transitiveRunSpecIds: Iterator[PathId] = transitiveAppIds ++ transitivePodIds
  //  def transitiveRunSpecsIterator: Iterator[RunSpec] = transitiveAppsIterator ++ transitivePodsIterator
  //  lazy val transitiveRunSpecs: Iterable[RunSpec] = new Iterable[RunSpec] {
  //    override def iterator: Iterator[RunSpec] = transitiveRunSpecsIterator
  //  }
  //  def transitiveRunSpecIdsIterator: Iterator[PathId] = transitiveAppIdsIterator ++ transitivePodIdsIterator
  //  lazy val transitiveRunSpecIds: Iterable[PathId] = new Iterable[PathId] {
  //    override def iterator: Iterator[GroupKey] = transitiveRunSpecIdsIterator
  //  }

  def transitiveGroupIds: Iterator[Group.GroupKey] = Iterator.single(this.id) ++ groupsById.valuesIterator.flatMap(_.transitiveGroupIds)
  def transitiveGroups: Iterator[Group] = Iterator.single(this) ++ groupsById.valuesIterator.flatMap(_.transitiveGroups)

  lazy val transitiveGroupsById: Map[Group.GroupKey, Group] = {
    Map(id -> this) ++ groupsById.values.flatMap(_.transitiveGroupsById)
  }

  /** @return true if and only if this group directly or indirectly contains app definitions. */
  def containsApps: Boolean = apps.nonEmpty || groupsById.exists { case (_, group) => group.containsApps }

  def containsPods: Boolean = pods.nonEmpty || groupsById.exists { case (_, group) => group.containsPods }

  def containsAppsOrPodsOrGroups: Boolean = apps.nonEmpty || groupsById.nonEmpty || pods.nonEmpty

  override def equals(other: Any): Boolean = other match {
    case that: Group =>
      id == that.id &&
        apps == that.apps &&
        pods == that.pods &&
        groupsById == that.groupsById &&
        dependencies == that.dependencies &&
        version == that.version
    case _ => false
  }

  override def hashCode(): Int = Objects.hash(id, apps, pods, groupsById, dependencies, version)

  override def toString = s"Group($id, ${apps.values}, ${pods.values}, ${groupsById.values}, $dependencies, $version)"
}

object Group {
  type GroupKey = PathId

  def apply(
    id: PathId,
    apps: Map[AppDefinition.AppKey, AppDefinition] = Group.defaultApps,
    pods: Map[PathId, PodDefinition] = Group.defaultPods,
    groupsById: Map[Group.GroupKey, Group] = Group.defaultGroups,
    dependencies: Set[PathId] = Group.defaultDependencies,
    version: Timestamp = Group.defaultVersion): Group =
    new Group(id, apps, pods, groupsById, dependencies, version)

  def empty(id: PathId): Group =
    Group(id = id, version = Timestamp(0))

  def defaultApps: Map[AppDefinition.AppKey, AppDefinition] = Map.empty
  val defaultPods = Map.empty[PathId, PodDefinition]
  def defaultGroups: Map[Group.GroupKey, Group] = Map.empty
  def defaultDependencies: Set[PathId] = Set.empty
  def defaultVersion: Timestamp = Timestamp.now()

  def validGroup(base: PathId, enabledFeatures: Set[String]): Validator[Group] =
    validator[Group] { group =>
      group.id is validPathWithBase(base)
      group.apps.values as "apps" is every(
        AppDefinition.validNestedAppDefinition(group.id.canonicalPath(base), enabledFeatures))
      group is noAppsAndPodsWithSameId
      group is noAppsAndGroupsWithSameName
      group is noPodsAndGroupsWithSameName
      group.groupsById.values as "groups" is every(validGroup(group.id.canonicalPath(base), enabledFeatures))
    }

  private def noAppsAndPodsWithSameId: Validator[Group] =
    isTrue("Applications and Pods may not share the same id") { group =>
      val podIds = group.transitivePodIds.to[Set]
      val appIds = group.transitiveAppIds.to[Set]
      appIds.intersect(podIds).isEmpty
    }

  private def noAppsAndGroupsWithSameName: Validator[Group] =
    isTrue("Groups and Applications may not have the same identifier.") { group =>
      val groupIds = group.groupsById.keySet
      val clashingIds = groupIds.intersect(group.apps.keySet)
      clashingIds.isEmpty
    }

  private def noPodsAndGroupsWithSameName: Validator[Group] =
    isTrue("Groups and Pods may not have the same identifier.") { group =>
      val groupIds = group.groupsById.keySet
      val clashingIds = groupIds.intersect(group.pods.keySet)
      clashingIds.isEmpty
    }

  def emptyUpdate(id: PathId): raml.GroupUpdate = raml.GroupUpdate(Some(id.toString))

  /** requires that apps are in canonical form */
  def validNestedGroupUpdateWithBase(base: PathId): Validator[raml.GroupUpdate] =
    validator[raml.GroupUpdate] { group =>
      group is notNull

      group.version is theOnlyDefinedOptionIn(group)
      group.scaleBy is theOnlyDefinedOptionIn(group)

      // this is funny: id is "optional" only because version and scaleBy can't be used in conjunction with other
      // fields. it feels like we should make an exception for "id" and always require it for non-root groups.
      group.id.map(_.toPath) as "id" is optional(valid)

      group.apps is optional(every(
        AppValidation.validNestedApp(group.id.fold(base)(PathId(_).canonicalPath(base)))))
      group.groups is optional(every(
        validNestedGroupUpdateWithBase(group.id.fold(base)(PathId(_).canonicalPath(base)))))
    }
}
