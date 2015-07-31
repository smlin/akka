/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster.sharding

import java.net.URLEncoder
import scala.collection.immutable
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.RootActorPath
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ddata._
import akka.cluster.ddata.Replicator._
import akka.cluster.Member
import akka.cluster.MemberStatus

/**
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
object ShardRegion {

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardRegion]] actor.
   */
  private[akka] def props(
    typeName: String,
    entityProps: Props,
    replicator: ActorRef,
    settings: ClusterShardingSettings,
    coordinatorPath: String,
    extractEntityId: ShardRegion.ExtractEntityId,
    extractShardId: ShardRegion.ExtractShardId,
    handOffStopMessage: Any): Props =
    Props(new ShardRegion(typeName, Some(entityProps), replicator, settings, coordinatorPath, extractEntityId,
      extractShardId, handOffStopMessage)).withDeploy(Deploy.local)

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardRegion]] actor
   * when using it in proxy only mode.
   */
  private[akka] def proxyProps(
    typeName: String,
    replicator: ActorRef,
    settings: ClusterShardingSettings,
    coordinatorPath: String,
    extractEntityId: ShardRegion.ExtractEntityId,
    extractShardId: ShardRegion.ExtractShardId): Props =
    Props(new ShardRegion(typeName, None, replicator, settings, coordinatorPath, extractEntityId, extractShardId, PoisonPill))
      .withDeploy(Deploy.local)

  /**
   * Marker type of region reference (`ActorRef`).
   */
  type RegionRef = ActorRef
  /**
   * Marker type of shard reference (`ActorRef`).
   */
  type ShardRef = ActorRef
  /**
   * Marker type of entity identifier (`String`).
   */
  type EntityId = String
  /**
   * Marker type of shard identifier (`String`).
   */
  type ShardId = String
  /**
   * Marker type of application messages (`Any`).
   */
  type Msg = Any
  /**
   * Interface of the partial function used by the [[ShardRegion]] to
   * extract the entity id and the message to send to the entity from an
   * incoming message. The implementation is application specific.
   * If the partial function does not match the message will be
   * `unhandled`, i.e. posted as `Unhandled` messages on the event stream.
   * Note that the extracted  message does not have to be the same as the incoming
   * message to support wrapping in message envelope that is unwrapped before
   * sending to the entity actor.
   */
  type ExtractEntityId = PartialFunction[Msg, (EntityId, Msg)]
  /**
   * Interface of the function used by the [[ShardRegion]] to
   * extract the shard id from an incoming message.
   * Only messages that passed the [[ExtractEntityId]] will be used
   * as input to this function.
   */
  type ExtractShardId = Msg ⇒ ShardId

  /**
   * Java API: Interface of functions to extract entity id,
   * shard id, and the message to send to the entity from an
   * incoming message.
   */
  trait MessageExtractor {
    /**
     * Extract the entity id from an incoming `message`. If `null` is returned
     * the message will be `unhandled`, i.e. posted as `Unhandled` messages on the event stream
     */
    def entityId(message: Any): String
    /**
     * Extract the message to send to the entity from an incoming `message`.
     * Note that the extracted message does not have to be the same as the incoming
     * message to support wrapping in message envelope that is unwrapped before
     * sending to the entity actor.
     */
    def entityMessage(message: Any): Any
    /**
     * Extract the entity id from an incoming `message`. Only messages that passed the [[#entityId]]
     * function will be used as input to this function.
     */
    def shardId(message: Any): String
  }

  /**
   * Convenience implementation of [[ShardRegion.MessageExtractor]] that
   * construct `shardId` based on the `hashCode` of the `entityId`. The number
   * of unique shards is limited by the given `maxNumberOfShards`.
   */
  abstract class HashCodeMessageExtractor(maxNumberOfShards: Int) extends MessageExtractor {
    /**
     * Default implementation pass on the message as is.
     */
    override def entityMessage(message: Any): Any = message

    override def shardId(message: Any): String =
      (math.abs(entityId(message).hashCode) % maxNumberOfShards).toString
  }

  sealed trait ShardRegionCommand

  /**
   * If the state of the entities are persistent you may stop entities that are not used to
   * reduce memory consumption. This is done by the application specific implementation of
   * the entity actors for example by defining receive timeout (`context.setReceiveTimeout`).
   * If a message is already enqueued to the entity when it stops itself the enqueued message
   * in the mailbox will be dropped. To support graceful passivation without loosing such
   * messages the entity actor can send this `Passivate` message to its parent `ShardRegion`.
   * The specified wrapped `stopMessage` will be sent back to the entity, which is
   * then supposed to stop itself. Incoming messages will be buffered by the `ShardRegion`
   * between reception of `Passivate` and termination of the entity. Such buffered messages
   * are thereafter delivered to a new incarnation of the entity.
   *
   * [[akka.actor.PoisonPill]] is a perfectly fine `stopMessage`.
   */
  @SerialVersionUID(1L) final case class Passivate(stopMessage: Any) extends ShardRegionCommand

  /*
   * Send this message to the `ShardRegion` actor to handoff all shards that are hosted by
   * the `ShardRegion` and then the `ShardRegion` actor will be stopped. You can `watch`
   * it to know when it is completed.
   */
  @SerialVersionUID(1L) final case object GracefulShutdown extends ShardRegionCommand

  /**
   * We must be sure that a shard is initialized before to start sending messages to it.
   * Shard could be terminated during initialization. It also required since the `Changed` event
   * from the `Replicator` arrives to a region with a big delay.
   */
  final case class ShardInitialized(shardId: ShardId)

  /**
   * Java API: Send this message to the `ShardRegion` actor to handoff all shards that are hosted by
   * the `ShardRegion` and then the `ShardRegion` actor will be stopped. You can `watch`
   * it to know when it is completed.
   */
  def gracefulShutdownInstance = GracefulShutdown

  /*
   * Send this message to the `ShardRegion` actor to request for [[CurrentRegions]],
   * which contains the addresses of all registered regions.
   * Intended for testing purpose to see when cluster sharding is "ready".
   */
  @SerialVersionUID(1L) final case object GetCurrentRegions extends ShardRegionCommand

  def getCurrentRegionsInstance = GetCurrentRegions

  /**
   * Reply to `GetCurrentRegions`
   */
  @SerialVersionUID(1L) final case class CurrentRegions(regions: Set[Address]) {
    /**
     * Java API
     */
    def getRegions: java.util.Set[Address] = {
      import scala.collection.JavaConverters._
      regions.asJava
    }

  }

  private case object Retry extends ShardRegionCommand

  /**
   * When an remembering entities and the shard stops unexpected (e.g. persist failure), we
   * restart it after a back off using this message.
   */
  private final case class RestartShard(shardId: ShardId)

  private def roleOption(role: String): Option[String] =
    if (role == "") None else Option(role)

  /**
   * INTERNAL API. Sends stopMessage (e.g. `PoisonPill`) to the entities and when all of
   * them have terminated it replies with `ShardStopped`.
   */
  private[akka] class HandOffStopper(shard: String, replyTo: ActorRef, entities: Set[ActorRef], stopMessage: Any)
    extends Actor {
    import ShardCoordinator.Internal.ShardStopped

    entities.foreach { a ⇒
      context watch a
      a ! stopMessage
    }

    var remaining = entities

    def receive = {
      case Terminated(ref) ⇒
        remaining -= ref
        if (remaining.isEmpty) {
          replyTo ! ShardStopped(shard)
          context stop self
        }
    }
  }

  private[akka] def handOffStopperProps(
    shard: String, replyTo: ActorRef, entities: Set[ActorRef], stopMessage: Any): Props =
    Props(new HandOffStopper(shard, replyTo, entities, stopMessage)).withDeploy(Deploy.local)
}

/**
 * This actor creates children entity actors on demand for the shards that it is told to be
 * responsible for. It delegates messages targeted to other shards to the responsible
 * `ShardRegion` actor on other nodes.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
class ShardRegion(
  typeName: String,
  entityProps: Option[Props],
  replicator: ActorRef,
  settings: ClusterShardingSettings,
  coordinatorPath: String,
  extractEntityId: ShardRegion.ExtractEntityId,
  extractShardId: ShardRegion.ExtractShardId,
  handOffStopMessage: Any) extends Actor with ActorLogging {

  import ShardCoordinator.Internal._
  import ShardRegion._
  import settings._
  import settings.tuningParameters._

  val cluster = Cluster(context.system)

  // sort by age, oldest first
  val ageOrdering = Ordering.fromLessThan[Member] { (a, b) ⇒ a.isOlderThan(b) }
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

  var regions = Map.empty[ActorRef, Set[ShardId]]
  var regionByShard = Map.empty[ShardId, RegionRef]
  var shardBuffers = Map.empty[ShardId, Vector[(Msg, ActorRef)]]
  var initializedShards = Map.empty[ShardId, ActorRef]
  var shardsByRef = Map.empty[ActorRef, ShardId]
  var handingOff = Set.empty[ActorRef]
  var gracefulShutdownInProgress = false

  implicit val node: Cluster = Cluster(context.system)
  // Map[ShardId, RegionRef]
  val ShardsStateKey = LWWMapKey[RegionRef](s"${typeName}ShardsState")

  getState()

  def totalBufferSize = shardBuffers.foldLeft(0) { (sum, entity) ⇒ sum + entity._2.size }

  import context.dispatcher
  val retryTask = context.system.scheduler.schedule(retryInterval, retryInterval, self, Retry)

  // subscribe to MemberEvent, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    retryTask.cancel()
  }

  def matchingRole(member: Member): Boolean = role match {
    case None    ⇒ true
    case Some(r) ⇒ member.hasRole(r)
  }

  def coordinatorSelection: Option[ActorSelection] =
    membersByAge.headOption.map(m ⇒ context.actorSelection(RootActorPath(m.address) + coordinatorPath))

  var coordinator: Option[ActorRef] = None

  def changeMembers(newMembers: immutable.SortedSet[Member]): Unit = {
    val before = membersByAge.headOption
    val after = newMembers.headOption
    membersByAge = newMembers
    if (before != after) {
      if (log.isDebugEnabled)
        log.debug("Coordinator moved from [{}] to [{}]", before.map(_.address).getOrElse(""), after.map(_.address).getOrElse(""))
      coordinator = None
      register()
    }
  }

  def receive = waitingForState

  def waitingForState: Receive = ({
    case g @ GetSuccess(ShardsStateKey, _) ⇒
      // regions in this point could be already terminated. Should it be equipped with a guaranteed delivery?
      changesReceived(g.get(ShardsStateKey).entries)
      activate()
    case NotFound(ShardsStateKey, _) ⇒ activate()
    case DataDeleted(ShardsStateKey) ⇒ activate()
  }: Receive).orElse(common)

  def active: Receive = ({
    case g @ Changed(ShardsStateKey) ⇒ changesReceived(g.get(ShardsStateKey).entries)
    case Terminated(ref)             ⇒ receiveTerminated(ref)
    case ShardInitialized(shardId)   ⇒ shardInitialized(shardId, sender())
    case msg: CoordinatorMessage     ⇒ receiveCoordinatorMessage(msg)
    case cmd: ShardRegionCommand     ⇒ receiveCommand(cmd)
    case msg: RestartShard           ⇒ deliverMessage(msg, sender())
  }: Receive).orElse(common)

  def common: Receive = {
    case evt: ClusterDomainEvent                 ⇒ receiveClusterEvent(evt)
    case state: CurrentClusterState              ⇒ receiveClusterState(state)
    case msg if extractEntityId.isDefinedAt(msg) ⇒ deliverMessage(msg, sender())
  }

  def getState(): Unit =
    replicator ! Get(ShardsStateKey, ReadLocal)

  def activate() = {
    context.become(active)
    replicator ! Subscribe(ShardsStateKey, self)
    log.info("ShardRegion was moved to the active state {}", regionByShard)
  }

  def changesReceived(regByShard: Map[ShardId, RegionRef]) = {
    regionByShard = regByShard.filterNot(_._2 == self)
    regions = regionByShard.groupBy(_._2).mapValues(_.keySet)
  }

  def removeShard(shardId: ShardId): Unit = {
    if (regionByShard.contains(shardId)) {
      val regionRef = regionByShard(shardId)
      val updatedShards = regions(regionRef) - shardId
      if (updatedShards.isEmpty) regions -= regionRef
      else regions = regions.updated(regionRef, updatedShards)
      regionByShard -= shardId
      replicator ! Update(ShardsStateKey, LWWMap.empty[RegionRef], WriteLocal)(_ - shardId)
    }
  }

  def receiveClusterState(state: CurrentClusterState): Unit = {
    changeMembers(immutable.SortedSet.empty(ageOrdering) ++ state.members.filter(m ⇒
      m.status == MemberStatus.Up && matchingRole(m)))
  }

  def receiveClusterEvent(evt: ClusterDomainEvent): Unit = evt match {
    case MemberUp(m) ⇒
      if (matchingRole(m))
        changeMembers(membersByAge + m)

    case MemberRemoved(m, _) ⇒
      if (m.uniqueAddress == cluster.selfUniqueAddress)
        context.stop(self)
      else if (matchingRole(m))
        changeMembers(membersByAge - m)

    case _ ⇒ unhandled(evt)
  }

  def receiveCoordinatorMessage(msg: CoordinatorMessage): Unit = msg match {
    case HostShard(shard) ⇒
      log.debug("Host Shard [{}] ", shard)

      //Start the shard, if already started this does nothing
      getShard(shard)

      // TODO: move to changesReceived method
      sender() ! ShardStarted(shard)

    case ShardHome(shard, ref) ⇒
      log.debug("Shard [{}] located at [{}]", shard, ref)
      regionByShard.get(shard) match {
        case Some(r) if r == self && ref != self ⇒
          // should not happen, inconsistency between ShardRegion and ShardCoordinator
          throw new IllegalStateException(s"Unexpected change of shard [${shard}] from self to [${ref}]")
        case _ ⇒
      }

      if (ref != self)
        context.watch(ref)

      if (ref == self)
        getShard(shard).foreach(deliverBufferedMessages(shard, _))
      else
        deliverBufferedMessages(shard, ref)

    case RegisterAck(coord) ⇒
      context.watch(coord)
      coordinator = Some(coord)
      requestShardBufferHomes()

    case BeginHandOff(shard) ⇒
      log.debug("BeginHandOff shard [{}]", shard)
      removeShard(shard)
      sender() ! BeginHandOffAck(shard)

    case msg @ HandOff(shard) ⇒
      log.debug("HandOff shard [{}]", shard)

      // must drop requests that came in between the BeginHandOff and now,
      // because they might be forwarded from other regions and there
      // is a risk or message re-ordering otherwise
      if (shardBuffers.contains(shard))
        shardBuffers -= shard

      if (initializedShards.contains(shard)) {
        handingOff += initializedShards(shard)
        initializedShards(shard) forward msg
      } else
        sender() ! ShardStopped(shard)

    case _ ⇒ unhandled(msg)

  }

  def receiveCommand(cmd: ShardRegionCommand): Unit = cmd match {
    case Retry ⇒
      if (coordinator.isEmpty)
        register()
      else {
        sendGracefulShutdownToCoordinator()
        requestShardBufferHomes()
      }

    case GracefulShutdown ⇒
      log.debug("Starting graceful shutdown of region and all its shards")
      gracefulShutdownInProgress = true
      sendGracefulShutdownToCoordinator()

    case GetCurrentRegions ⇒
      coordinator match {
        case Some(c) ⇒ c.forward(GetCurrentRegions)
        case None    ⇒ sender() ! CurrentRegions(Set.empty)
      }

    case _ ⇒ unhandled(cmd)
  }

  def receiveTerminated(ref: ActorRef): Unit = {
    if (coordinator.exists(_ == ref))
      coordinator = None
    else if (regions.contains(ref)) {
      // TODO: doing this in coordinator
      val shards = regions(ref)
      regionByShard --= shards
      regions -= ref
      if (log.isDebugEnabled)
        log.debug("Region [{}] with shards [{}] terminated", ref, shards.mkString(", "))
    } else if (shardsByRef.contains(ref)) {
      val shardId: ShardId = shardsByRef(ref)

      shardsByRef = shardsByRef - ref
      initializedShards = initializedShards - shardId
      if (handingOff.contains(ref)) {
        handingOff = handingOff - ref
        log.debug("Shard [{}] handoff complete", shardId)
      } else {
        // if persist fails it will stop
        log.debug("Shard [{}]  terminated while not being handed off", shardId)
        if (rememberEntities) {
          context.system.scheduler.scheduleOnce(shardFailureBackoff, self, RestartShard(shardId))
        }
      }

      if (gracefulShutdownInProgress && initializedShards.isEmpty && shardBuffers.isEmpty)
        context.stop(self) // all shards have been rebalanced, complete graceful shutdown
    }
  }

  def register(): Unit = {
    coordinatorSelection.foreach(_ ! registrationMessage)
  }

  def registrationMessage: Any =
    if (entityProps.isDefined) Register(self) else RegisterProxy(self)

  def requestShardBufferHomes(): Unit = {
    shardBuffers.foreach {
      case (shard, _) ⇒ coordinator.foreach { c ⇒
        log.debug("Retry request for shard [{}] homes", shard)
        c ! GetShardHome(shard)
      }
    }
  }

  def shardInitialized(id: ShardId, shard: ActorRef): Unit = {
    log.debug("Shard was initialized {}", id)
    initializedShards = initializedShards.updated(id, shard)
    deliverBufferedMessages(id, shard)
  }

  def bufferMessage(shardId: ShardId, msg: Any, snd: ActorRef) = {
    if (totalBufferSize >= bufferSize) {
      log.debug("Buffer is full, dropping message for shard [{}]", shardId)
      context.system.deadLetters ! msg
    } else {
      val buf = shardBuffers.getOrElse(shardId, Vector.empty)
      shardBuffers = shardBuffers.updated(shardId, buf :+ ((msg, snd)))
    }
  }

  def deliverBufferedMessages(shardId: ShardId, receiver: ActorRef): Unit = {
    shardBuffers.get(shardId) match {
      case Some(buf) ⇒
        buf.foreach { case (msg, snd) ⇒ receiver.tell(msg, snd) }
        shardBuffers -= shardId
      case None ⇒
    }
  }

  def deliverMessage(msg: Any, snd: ActorRef): Unit =
    msg match {
      case RestartShard(shardId) ⇒
        regionByShard.get(shardId) match {
          case Some(ref) ⇒
            if (ref == self)
              getShard(shardId)
          case None ⇒
            if (!shardBuffers.contains(shardId)) {
              log.debug("Request shard [{}] home", shardId)
              coordinator.foreach(_ ! GetShardHome(shardId))
            }
            val buf = shardBuffers.getOrElse(shardId, Vector.empty)
            shardBuffers = shardBuffers.updated(shardId, buf :+ ((msg, snd)))
        }

      case _ ⇒
        val shardId = extractShardId(msg)
        regionByShard.get(shardId) match {
          case Some(ref) if ref == self ⇒
            getShard(shardId) match {
              case Some(shard) ⇒
                shardBuffers.get(shardId) match {
                  case Some(buf) ⇒
                    // Since now messages to a shard is buffered then those messages must be in right order
                    bufferMessage(shardId, msg, snd)
                    deliverBufferedMessages(shardId, shard)
                  case None ⇒
                    shard.tell(msg, snd)
                }
              case None ⇒ bufferMessage(shardId, msg, snd)
            }
          case Some(ref) ⇒
            log.debug("Forwarding request for shard [{}] to [{}]", shardId, ref)
            ref.tell(msg, snd)
          case None if (shardId == null || shardId == "") ⇒
            log.warning("Shard must not be empty, dropping message [{}]", msg.getClass.getName)
            context.system.deadLetters ! msg
          case None ⇒
            if (!shardBuffers.contains(shardId)) {
              log.debug("Request shard [{}] home", shardId)
              coordinator.foreach(_ ! GetShardHome(shardId))
            }
            bufferMessage(shardId, msg, snd)
        }
    }

  def getShard(id: ShardId): Option[ActorRef] = initializedShards.get(id).orElse(
    entityProps match {
      case Some(props) if !shardsByRef.values.exists(_ == id) ⇒
        log.debug("Starting shard [{}] in region", id)

        val name = URLEncoder.encode(id, "utf-8")
        val shard = context.watch(context.actorOf(
          Shard.props(
            typeName,
            id,
            replicator,
            props,
            settings,
            extractEntityId,
            extractShardId,
            handOffStopMessage).withDispatcher(context.props.dispatcher),
          name))
        shardsByRef = shardsByRef.updated(shard, id)
        None
      case Some(props) ⇒
        None
      case None ⇒
        throw new IllegalStateException("Shard must not be allocated to a proxy only ShardRegion")
    })

  def sendGracefulShutdownToCoordinator(): Unit =
    if (gracefulShutdownInProgress)
      coordinator.foreach(_ ! GracefulShutdownReq(self))
}
