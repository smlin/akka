/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster.sharding

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Deploy
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.Stash
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterShuttingDown
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.Replicator._
import akka.dispatch.ExecutionContexts
import akka.pattern.pipe

/**
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
object ShardCoordinator {

  import ShardRegion.ShardId

  /**
   * INTERNAL API
   * Factory method for the [[akka.actor.Props]] of the [[ShardCoordinator]] actor.
   */
  private[akka] def props(typeName: String, settings: ClusterShardingSettings,
                          allocationStrategy: ShardAllocationStrategy,
                          replicator: ActorRef): Props =
    Props(new ShardCoordinator(typeName: String, settings, allocationStrategy, replicator)).withDeploy(Deploy.local)

  /**
   * Interface of the pluggable shard allocation and rebalancing logic used by the [[ShardCoordinator]].
   *
   * Java implementations should extend [[AbstractShardAllocationStrategy]].
   */
  trait ShardAllocationStrategy extends NoSerializationVerificationNeeded {
    /**
     * Invoked when the location of a new shard is to be decided.
     * @param requester actor reference to the [[ShardRegion]] that requested the location of the
     *   shard, can be returned if preference should be given to the node where the shard was first accessed
     * @param shardId the id of the shard to allocate
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *   in the order they were allocated
     * @return a `Future` of the actor ref of the [[ShardRegion]] that is to be responsible for the shard, must be one of
     *   the references included in the `currentShardAllocations` parameter
     */
    def allocateShard(requester: ActorRef, shardId: ShardId,
                      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef]

    /**
     * Invoked periodically to decide which shards to rebalance to another location.
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *   in the order they were allocated
     * @param rebalanceInProgress set of shards that are currently being rebalanced, i.e.
     *   you should not include these in the returned set
     * @return a `Future` of the shards to be migrated, may be empty to skip rebalance in this round
     */
    def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                  rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]]
  }

  /**
   * Java API: Java implementations of custom shard allocation and rebalancing logic used by the [[ShardCoordinator]]
   * should extend this abstract class and implement the two methods.
   */
  abstract class AbstractShardAllocationStrategy extends ShardAllocationStrategy {
    override final def allocateShard(requester: ActorRef, shardId: ShardId,
                                     currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {

      import scala.collection.JavaConverters._
      allocateShard(requester, shardId, currentShardAllocations.asJava)
    }

    override final def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                                 rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {
      import scala.collection.JavaConverters._
      implicit val ec = ExecutionContexts.sameThreadExecutionContext
      rebalance(currentShardAllocations.asJava, rebalanceInProgress.asJava).map(_.asScala.toSet)
    }

    /**
     * Invoked when the location of a new shard is to be decided.
     * @param requester actor reference to the [[ShardRegion]] that requested the location of the
     *   shard, can be returned if preference should be given to the node where the shard was first accessed
     * @param shardId the id of the shard to allocate
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *   in the order they were allocated
     * @return a `Future` of the actor ref of the [[ShardRegion]] that is to be responsible for the shard, must be one of
     *   the references included in the `currentShardAllocations` parameter
     */
    def allocateShard(requester: ActorRef, shardId: String,
                      currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]]): Future[ActorRef]

    /**
     * Invoked periodically to decide which shards to rebalance to another location.
     * @param currentShardAllocations all actor refs to `ShardRegion` and their current allocated shards,
     *   in the order they were allocated
     * @param rebalanceInProgress set of shards that are currently being rebalanced, i.e.
     *   you should not include these in the returned set
     * @return a `Future` of the shards to be migrated, may be empty to skip rebalance in this round
     */
    def rebalance(currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]],
                  rebalanceInProgress: java.util.Set[String]): Future[java.util.Set[String]]
  }

  private val emptyRebalanceResult = Future.successful(Set.empty[ShardId])

  /**
   * The default implementation of [[ShardCoordinator.LeastShardAllocationStrategy]]
   * allocates new shards to the `ShardRegion` with least number of previously allocated shards.
   * It picks shards for rebalancing handoff from the `ShardRegion` with most number of previously allocated shards.
   * They will then be allocated to the `ShardRegion` with least number of previously allocated shards,
   * i.e. new members in the cluster. There is a configurable threshold of how large the difference
   * must be to begin the rebalancing. The number of ongoing rebalancing processes can be limited.
   */
  @SerialVersionUID(1L)
  class LeastShardAllocationStrategy(rebalanceThreshold: Int, maxSimultaneousRebalance: Int)
    extends ShardAllocationStrategy with Serializable {

    override def allocateShard(requester: ActorRef, shardId: ShardId,
                               currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {
      val (regionWithLeastShards, _) = currentShardAllocations.minBy { case (_, v) ⇒ v.size }
      Future.successful(regionWithLeastShards)
    }

    override def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                           rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {
      if (rebalanceInProgress.size < maxSimultaneousRebalance) {
        val (regionWithLeastShards, leastShards) = currentShardAllocations.minBy { case (_, v) ⇒ v.size }
        val mostShards = currentShardAllocations.collect {
          case (_, v) ⇒ v.filterNot(s ⇒ rebalanceInProgress(s))
        }.maxBy(_.size)
        if (mostShards.size - leastShards.size >= rebalanceThreshold)
          Future.successful(Set(mostShards.head))
        else
          emptyRebalanceResult
      } else emptyRebalanceResult
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    /**
     * Messages sent to the coordinator
     */
    sealed trait CoordinatorCommand
    /**
     * Messages sent from the coordinator
     */
    sealed trait CoordinatorMessage
    /**
     * `ShardRegion` registers to `ShardCoordinator`, until it receives [[RegisterAck]].
     */
    @SerialVersionUID(1L) final case class Register(shardRegion: ActorRef) extends CoordinatorCommand
    /**
     * `ShardRegion` in proxy only mode registers to `ShardCoordinator`, until it receives [[RegisterAck]].
     */
    @SerialVersionUID(1L) final case class RegisterProxy(shardRegionProxy: ActorRef) extends CoordinatorCommand
    /**
     * Acknowledgement from `ShardCoordinator` that [[Register]] or [[RegisterProxy]] was successful.
     */
    @SerialVersionUID(1L) final case class RegisterAck(coordinator: ActorRef) extends CoordinatorMessage
    /**
     * `ShardRegion` requests the location of a shard by sending this message
     * to the `ShardCoordinator`.
     */
    @SerialVersionUID(1L) final case class GetShardHome(shard: ShardId) extends CoordinatorCommand
    /**
     * `ShardCoordinator` replies with this message for [[GetShardHome]] requests.
     */
    @SerialVersionUID(1L) final case class ShardHome(shard: ShardId, ref: ActorRef) extends CoordinatorMessage
    /**
     * `ShardCoordinator` informs a `ShardRegion` that it is hosting this shard
     */
    @SerialVersionUID(1L) final case class HostShard(shard: ShardId) extends CoordinatorMessage
    /**
     * `ShardRegion` replies with this message for [[HostShard]] requests which lead to it hosting the shard
     */
    @SerialVersionUID(1l) final case class ShardStarted(shard: ShardId) extends CoordinatorMessage
    /**
     * `ShardCoordinator` initiates rebalancing process by sending this message
     * to all registered `ShardRegion` actors (including proxy only). They are
     * supposed to discard their known location of the shard, i.e. start buffering
     * incoming messages for the shard. They reply with [[BeginHandOffAck]].
     * When all have replied the `ShardCoordinator` continues by sending
     * `HandOff` to the `ShardRegion` responsible for the shard.
     */
    @SerialVersionUID(1L) final case class BeginHandOff(shard: ShardId) extends CoordinatorMessage
    /**
     * Acknowledgement of [[BeginHandOff]]
     */
    @SerialVersionUID(1L) final case class BeginHandOffAck(shard: ShardId) extends CoordinatorCommand
    /**
     * When all `ShardRegion` actors have acknowledged the `BeginHandOff` the
     * `ShardCoordinator` sends this message to the `ShardRegion` responsible for the
     * shard. The `ShardRegion` is supposed to stop all entities in that shard and when
     * all entities have terminated reply with `ShardStopped` to the `ShardCoordinator`.
     */
    @SerialVersionUID(1L) final case class HandOff(shard: ShardId) extends CoordinatorMessage
    /**
     * Reply to `HandOff` when all entities in the shard have been terminated.
     */
    @SerialVersionUID(1L) final case class ShardStopped(shard: ShardId) extends CoordinatorCommand

    /**
     * Result of `allocateShard` is piped to self with this message.
     */
    @SerialVersionUID(1L) final case class AllocateShardResult(
      shard: ShardId, shardRegion: Option[ActorRef], getShardHomeSender: ActorRef) extends CoordinatorCommand

    /**
     * Result of `rebalance` is piped to self with this message.
     */
    @SerialVersionUID(1L) final case class RebalanceResult(shards: Set[ShardId]) extends CoordinatorCommand

    /**
     * `ShardRegion` requests full handoff to be able to shutdown gracefully.
     */
    @SerialVersionUID(1L) final case class GracefulShutdownReq(shardRegion: ActorRef) extends CoordinatorCommand

    // DomainEvents for the persistent state of the event sourced ShardCoordinator
    sealed trait DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionRegistered(region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionProxyRegistered(regionProxy: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionTerminated(region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardRegionProxyTerminated(regionProxy: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardHomeAllocated(shard: ShardId, region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) final case class ShardHomeDeallocated(shard: ShardId) extends DomainEvent

    object State {
      val empty = State()
    }

    /**
     * Persistent state of the event sourced ShardCoordinator.
     */
    @SerialVersionUID(1L) final case class State private (
      // region for each shard
      shards: Map[ShardId, ActorRef] = Map.empty,
      // shards for each region
      regions: Map[ActorRef, Vector[ShardId]] = Map.empty,
      regionProxies: Set[ActorRef] = Set.empty,
      unallocatedShards: Set[ShardId] = Set.empty) {

      def updated(event: DomainEvent): State = event match {
        case ShardRegionRegistered(region) ⇒
          require(!regions.contains(region), s"Region $region already registered: $this")
          copy(regions = regions.updated(region, Vector.empty))
        case ShardRegionProxyRegistered(proxy) ⇒
          require(!regionProxies.contains(proxy), s"Region proxy $proxy already registered: $this")
          copy(regionProxies = regionProxies + proxy)
        case ShardRegionTerminated(region) ⇒
          require(regions.contains(region), s"Terminated region $region not registered: $this")
          copy(
            regions = regions - region,
            shards = shards -- regions(region),
            unallocatedShards = unallocatedShards ++ regions(region))
        case ShardRegionProxyTerminated(proxy) ⇒
          require(regionProxies.contains(proxy), s"Terminated region proxy $proxy not registered: $this")
          copy(regionProxies = regionProxies - proxy)
        case ShardHomeAllocated(shard, region) ⇒
          require(regions.contains(region), s"Region $region not registered: $this")
          require(!shards.contains(shard), s"Shard [$shard] already allocated: $this")
          copy(
            shards = shards.updated(shard, region),
            regions = regions.updated(region, regions(region) :+ shard),
            unallocatedShards = unallocatedShards - shard)
        case ShardHomeDeallocated(shard) ⇒
          require(shards.contains(shard), s"Shard [$shard] not allocated: $this")
          val region = shards(shard)
          require(regions.contains(region), s"Region $region for shard [$shard] not registered: $this")
          copy(
            shards = shards - shard,
            regions = regions.updated(region, regions(region).filterNot(_ == shard)),
            unallocatedShards = unallocatedShards + shard)
      }
    }

  }

  /**
   * Periodic message to trigger rebalance
   */
  private case object RebalanceTick
  /**
   * End of rebalance process performed by [[RebalanceWorker]]
   */
  private final case class RebalanceDone(shard: ShardId, ok: Boolean)
  /**
   * Check if we've received a shard start request
   */
  private final case class ResendShardHost(shard: ShardId, region: ActorRef)

  private final case class DelayedShardRegionTerminated(region: ActorRef)

  /**
   * INTERNAL API. Rebalancing process is performed by this actor.
   * It sends `BeginHandOff` to all `ShardRegion` actors followed by
   * `HandOff` to the `ShardRegion` responsible for the shard.
   * When the handoff is completed it sends [[RebalanceDone]] to its
   * parent `ShardCoordinator`. If the process takes longer than the
   * `handOffTimeout` it also sends [[RebalanceDone]].
   */
  private[akka] class RebalanceWorker(shard: String, from: ActorRef, handOffTimeout: FiniteDuration,
                                      regions: Set[ActorRef]) extends Actor {
    import Internal._
    regions.foreach(_ ! BeginHandOff(shard))
    var remaining = regions

    import context.dispatcher
    context.system.scheduler.scheduleOnce(handOffTimeout, self, ReceiveTimeout)

    def receive = {
      case BeginHandOffAck(`shard`) ⇒
        remaining -= sender()
        if (remaining.isEmpty) {
          from ! HandOff(shard)
          context.become(stoppingShard, discardOld = true)
        }
      case ReceiveTimeout ⇒ done(ok = false)
    }

    def stoppingShard: Receive = {
      case ShardStopped(shard) ⇒ done(ok = true)
      case ReceiveTimeout      ⇒ done(ok = false)
    }

    def done(ok: Boolean): Unit = {
      context.parent ! RebalanceDone(shard, ok)
      context.stop(self)
    }
  }

  private[akka] def rebalanceWorkerProps(shard: String, from: ActorRef, handOffTimeout: FiniteDuration,
                                         regions: Set[ActorRef]): Props =
    Props(new RebalanceWorker(shard, from, handOffTimeout, regions))

}

/**
 * Singleton coordinator that decides where to allocate shards.
 *
 * @see [[ClusterSharding$ ClusterSharding extension]]
 */
class ShardCoordinator(typeName: String, settings: ClusterShardingSettings,
                       allocationStrategy: ShardCoordinator.ShardAllocationStrategy,
                       replicator: ActorRef)
  extends Actor with ActorLogging with Stash {
  import ShardCoordinator._
  import ShardCoordinator.Internal._
  import ShardRegion.ShardId
  import settings.tuningParameters._

  val removalMargin = Cluster(context.system).settings.DownRemovalMargin
  val waitingForStateTimeout = settings.tuningParameters.waitingForStateTimeout
  val updatingStateTimeout = settings.tuningParameters.updatingStateTimeout

  var state = State.empty
  var rebalanceInProgress = Set.empty[ShardId]
  var unAckedHostShards = Map.empty[ShardId, Cancellable]
  // regions that have requested handoff, for graceful shutdown
  var gracefulShutdownInProgress = Set.empty[ActorRef]
  var aliveRegions = Set.empty[ActorRef]

  import context.dispatcher
  val rebalanceTask = context.system.scheduler.schedule(rebalanceInterval, rebalanceInterval, self, RebalanceTick)

  Cluster(context.system).subscribe(self, ClusterShuttingDown.getClass)

  implicit val node = Cluster(context.system)
  val CoordinatorStateKey = LWWRegisterKey[State](s"${typeName}CoordinatorState")

  getState()

  override def postStop(): Unit = {
    super.postStop()
    rebalanceTask.cancel()
    Cluster(context.system).unsubscribe(self)
  }

  override def receive: Receive = waitingForState

  def waitingForState: Receive = {
    case g @ GetSuccess(CoordinatorStateKey, _) ⇒
      state = g.get(CoordinatorStateKey).value
      state.regionProxies.foreach(context.watch)
      state.regions.foreach { case (a, _) ⇒ context.watch(a) }
      state.shards.foreach { case (a, r) ⇒ sendHostShardMsg(a, r) }
      allocateShardHomes()
      activate()

    case GetFailure(CoordinatorStateKey, _) ⇒
      log.error(
        "The ShardCoordinator was unable to get an initial state within 'waiting-for-state-timeout' (was retrying): {} millis",
        waitingForStateTimeout.toMillis)
      getState()

    case DataDeleted(CoordinatorStateKey) ⇒ activate()
    case NotFound(CoordinatorStateKey, _) ⇒ activate()
  }

  def active: Receive = {
    case UpdateSuccess(CoordinatorStateKey, Some(evt: DomainEvent)) ⇒
      log.debug("The coordinator state was successfully updated with {}", evt)
      evt match {
        case ShardRegionRegistered(region) ⇒
          if (state.regions.contains(region))
            sender() ! RegisterAck(self)
          else {
            val firstRegion = state.regions.isEmpty

            state = state.updated(evt)
            context.watch(region)
            region ! RegisterAck(self)

            if (firstRegion)
              allocateShardHomes()
          }
        case _ ⇒
      }

    case UpdateTimeout(CoordinatorStateKey, Some(evt: DomainEvent)) ⇒
      evt match {
        case ShardRegionRegistered(region) ⇒
          log.warning(
            "The ShardCoordinator was unable to update a distributed state within 'updating-state-timeout'={} millis, event={}",
            updatingStateTimeout.toMillis,
            evt)
        case _ ⇒
          log.error(
            "The ShardCoordinator was unable to update a distributed state within 'updating-state-timeout'={} millis (was retrying), event={}",
            updatingStateTimeout.toMillis,
            evt)
          sendUpdate(evt)
      }

    case ModifyFailure(CoordinatorStateKey, error, cause, Some(evt: DomainEvent)) ⇒
      log.error(
        cause,
        "The ShardCoordinator was unable to update a distributed state with error {}. Was retrying with event {}.",
        error,
        evt)
      sendUpdate(evt)

    case Register(region) ⇒
      log.debug("ShardRegion registered: [{}]", region)
      aliveRegions += region
      if (state.regions.contains(region))
        sender() ! RegisterAck(self)
      else {
        gracefulShutdownInProgress -= region
        sendUpdate(ShardRegionRegistered(region))
      }

    case RegisterProxy(proxy) ⇒
      log.debug("ShardRegion proxy registered: [{}]", proxy)
      if (state.regionProxies.contains(proxy))
        sender() ! RegisterAck(self)
      else {
        update(ShardRegionProxyRegistered(proxy))
        context.watch(proxy)
        sender() ! RegisterAck(self)
      }

    case t @ Terminated(ref) ⇒
      if (state.regions.contains(ref)) {
        if (removalMargin != Duration.Zero && t.addressTerminated && aliveRegions(ref))
          context.system.scheduler.scheduleOnce(removalMargin, self, DelayedShardRegionTerminated(ref))
        else
          regionTerminated(ref)
      } else if (state.regionProxies.contains(ref)) {
        log.debug("ShardRegion proxy terminated: [{}]", ref)
        update(ShardRegionProxyTerminated(ref))
      }

    case DelayedShardRegionTerminated(ref) ⇒
      regionTerminated(ref)

    case GetShardHome(shard) ⇒
      if (!rebalanceInProgress.contains(shard)) {
        state.shards.get(shard) match {
          case Some(ref) ⇒ sender() ! ShardHome(shard, ref)
          case None ⇒
            val activeRegions = state.regions -- gracefulShutdownInProgress
            if (activeRegions.nonEmpty) {
              val getShardHomeSender = sender()
              val regionFuture = allocationStrategy.allocateShard(getShardHomeSender, shard, activeRegions)
              regionFuture.value match {
                case Some(Success(region)) ⇒
                  continueGetShardHome(shard, region, getShardHomeSender)
                case _ ⇒
                  // continue when future is completed
                  regionFuture.map { region ⇒
                    AllocateShardResult(shard, Some(region), getShardHomeSender)
                  }.recover {
                    case _ ⇒ AllocateShardResult(shard, None, getShardHomeSender)
                  }.pipeTo(self)
              }
            }
        }
      }

    case AllocateShardResult(shard, None, getShardHomeSender) ⇒
      log.debug("Shard [{}] allocation failed. It will be retried.", shard)

    case AllocateShardResult(shard, Some(region), getShardHomeSender) ⇒
      continueGetShardHome(shard, region, getShardHomeSender)

    case ShardStarted(shard) ⇒
      unAckedHostShards.get(shard) match {
        case Some(cancel) ⇒
          cancel.cancel()
          unAckedHostShards = unAckedHostShards - shard
        case _ ⇒
      }

    case ResendShardHost(shard, region) ⇒
      state.shards.get(shard) match {
        case Some(`region`) ⇒ sendHostShardMsg(shard, region)
        case _              ⇒ //Reallocated to another region
      }

    case RebalanceTick ⇒
      if (state.regions.nonEmpty) {
        val shardsFuture = allocationStrategy.rebalance(state.regions, rebalanceInProgress)
        shardsFuture.value match {
          case Some(Success(shards)) ⇒
            continueRebalance(shards)
          case _ ⇒
            // continue when future is completed
            shardsFuture.map { shards ⇒ RebalanceResult(shards)
            }.recover {
              case _ ⇒ RebalanceResult(Set.empty)
            }.pipeTo(self)
        }
      }

    case RebalanceResult(shards) ⇒
      continueRebalance(shards)

    case RebalanceDone(shard, ok) ⇒
      rebalanceInProgress -= shard
      log.debug("Rebalance shard [{}] done [{}]", shard, ok)
      // The shard could have been removed by ShardRegionTerminated
      if (state.shards.contains(shard))
        if (ok) {
          update(ShardHomeDeallocated(shard))
          log.debug("Shard [{}] deallocated", shard)
          allocateShardHomes()
        } else // rebalance not completed, graceful shutdown will be retried
          gracefulShutdownInProgress -= state.shards(shard)

    case GracefulShutdownReq(region) ⇒
      if (!gracefulShutdownInProgress(region))
        state.regions.get(region) match {
          case Some(shards) ⇒
            log.debug("Graceful shutdown of region [{}] with shards [{}]", region, shards)
            gracefulShutdownInProgress += region
            continueRebalance(shards.toSet)
          case None ⇒
        }

    case ShardHome(_, _) ⇒
    //On rebalance, we send ourselves a GetShardHome message to reallocate a
    // shard. This receive handles the "response" from that message. i.e. ignores it.

    case ClusterShuttingDown ⇒
      log.debug("Shutting down ShardCoordinator")
      // can't stop because supervisor will start it again,
      // it will soon be stopped when singleton is stopped
      context.become(shuttingDown)

    case ShardRegion.GetCurrentRegions ⇒
      val reply = ShardRegion.CurrentRegions(state.regions.keySet.map { ref ⇒
        if (ref.path.address.host.isEmpty) Cluster(context.system).selfAddress
        else ref.path.address
      })
      sender() ! reply

    case _: CurrentClusterState ⇒
  }

  def activate() = {
    context.become(active)
    unstashAll()
    log.info("Sharding Coordinator was moved to the active state {}", state)
  }

  def update(evt: DomainEvent): Unit = {
    state = state.updated(evt)
    sendUpdate(evt)
  }

  def getState(): Unit =
    replicator ! Get(CoordinatorStateKey, ReadMajority(waitingForStateTimeout))

  def sendUpdate(evt: DomainEvent) = {
    val s = state
    replicator ! Update(CoordinatorStateKey, LWWRegister(State.empty), WriteMajority(updatingStateTimeout), Some(evt)) { reg ⇒
      reg.withValue(s)
    }
  }

  def regionTerminated(ref: ActorRef): Unit =
    if (state.regions.contains(ref)) {
      log.debug("ShardRegion terminated: [{}]", ref)
      state.regions(ref).foreach { s ⇒ self ! GetShardHome(s) }

      gracefulShutdownInProgress -= ref

      update(ShardRegionTerminated(ref))
      allocateShardHomes()
    }

  def shuttingDown: Receive = {
    case _ ⇒ // ignore all
  }

  def sendHostShardMsg(shard: ShardId, region: ActorRef): Unit = {
    region ! HostShard(shard)
    val cancel = context.system.scheduler.scheduleOnce(shardStartTimeout, self, ResendShardHost(shard, region))
    unAckedHostShards = unAckedHostShards.updated(shard, cancel)
  }

  def allocateShardHomes(): Unit = state.unallocatedShards.foreach { self ! GetShardHome(_) }

  def continueGetShardHome(shard: ShardId, region: ActorRef, getShardHomeSender: ActorRef): Unit =
    if (!rebalanceInProgress.contains(shard)) {
      state.shards.get(shard) match {
        case Some(ref) ⇒ getShardHomeSender ! ShardHome(shard, ref)
        case None ⇒
          if (state.regions.contains(region) && !gracefulShutdownInProgress.contains(region)) {
            update(ShardHomeAllocated(shard, region))
            log.debug("Shard [{}] allocated at [{}]", shard, region)

            sendHostShardMsg(shard, region)
            getShardHomeSender ! ShardHome(shard, region)
          } else
            log.debug("Allocated region {} for shard [{}] is not (any longer) one of the registered regions: {}",
              region, shard, state)
      }
    }

  def continueRebalance(shards: Set[ShardId]): Unit =
    shards.foreach { shard ⇒
      if (!rebalanceInProgress(shard)) {
        state.shards.get(shard) match {
          case Some(rebalanceFromRegion) ⇒
            rebalanceInProgress += shard
            log.debug("Rebalance shard [{}] from [{}]", shard, rebalanceFromRegion)
            context.actorOf(rebalanceWorkerProps(shard, rebalanceFromRegion, handOffTimeout,
              state.regions.keySet ++ state.regionProxies)
              .withDispatcher(context.props.dispatcher))
          case None ⇒
            log.debug("Rebalance of non-existing shard [{}] is ignored", shard)
        }

      }
    }

}
