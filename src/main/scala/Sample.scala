package akka.cluster

import language._
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import akka.actor._
import akka.cluster._
import akka.cluster.ClusterEvent._
import akka.pattern._
import akka.util._
import akka.event._
import akka.event.Logging._
import akka.io._
import com.typesafe.config.ConfigFactory
import scala.concurrent._
import spray.can._
import spray.can.server._
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._

//#messages
case class TransformationJob(text: String)
case class TransformationResult(text: String)
case class JobFailed(reason: String, job: TransformationJob)
case object BackendRegistration
//#messages

object ConsoleSample {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val config =
      (if (args.nonEmpty) ConfigFactory.parseString(s"akka.remote.netty.tcp.port=${args(0)}")
      else ConfigFactory.empty).withFallback(
        ConfigFactory.parseString("akka.cluster.roles = [console]")).
        withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val console = system.actorOf(Props[ClusterConsole], name = "console")
  }
}

class ClusterConsole extends Actor {

  implicit val actSystem = context.system
  akka.io.IO(Http) ! Http.Bind(self, interface = "localhost", port = 8080)

  val cluster = Cluster(context.system)
  var logs = new ListBuffer[LogEvent]()

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[Logging.LogEvent])
    cluster.subscribe(self, classOf[ClusterDomainEvent])
  }
  override def postStop(): Unit =  {
    context.system.eventStream.unsubscribe(self)
  }

  class ClusterInformation (val isAvailable: Boolean, 
    val isSingletonCluster: Boolean, 
    val leader: Option[Address],
    val members: Seq[Member],
    val unreachableMembers: Seq[Member],
    val logs: Seq[LogEvent])

  def index(info: ClusterInformation) = HttpResponse(
    entity = HttpEntity(`text/html`,
      <html>
        <body>
          <h1>Cluster Status</h1>
          <ul>
            <li>Is available: {info.isAvailable}</li>
            <li>Leader: {info.leader.toString}</li>
            <li>Is singleton cluster: {info.isSingletonCluster}</li>
            <li>Members: {info.members.size}
              <ul>
                {formatToHtml("<li>", info.members, "</li>")}
              </ul>
            </li>
            <li> Unreachable members: {info.unreachableMembers.size}
              <ul>
                {formatToHtml("<li>", info.unreachableMembers, "</li>")}
              </ul>
            </li>
            <li>Logs: {info.logs.size}
              <ul>
                {formatToHtml("<li>", info.logs, "</li>")}
              </ul>
            </li>
          </ul>
        </body>
      </html>.toString()
    )
  )

  def formatToHtml(first: String, items: Seq[_], last: String) = {
    var output: String = ""
    items.foreach { item => output += first + item.toString + last}
    scala.xml.Unparsed(output)
  }

  def receive = {    
    case _: Http.Connected => sender ! Http.Register(self)
    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index(new ClusterInformation(isClusterAvailable, isSingletonCluster, getLeader, getMembers, getUnreachableMembers, logs.reverse.toSeq))
    case InitializeLogger(_) => sender ! LoggerInitialized
    case Error(cause, logSource, logClass, message) => 
      eventErrorLog(new Logging.Error(cause, logSource, logClass, message))
    case Warning(logSource, logClass, message) => 
      eventWarningLog(new Logging.Warning(logSource, logClass, message))
    case Info(logSource, logClass, message) => 
      eventInfoLog(new Logging.Info(logSource, logClass, message))
    case Debug(logSource, logClass, message) => 
      eventDebugLog(new Logging.Debug(logSource, logClass, message))
  }


  val clusterView = cluster.readView

  def getMembers = clusterView.members.toSeq
  def getUnreachableMembers = clusterView.unreachableMembers.toSeq
  def getLeader = clusterView.leader
  def isSingletonCluster = clusterView.isSingletonCluster
  def isClusterAvailable = clusterView.isAvailable


  def eventErrorLog(log: Logging.Error) = defaultLog(log)

  def eventWarningLog(log: Logging.Warning) = defaultLog(log)

  def eventInfoLog(log: Logging.Info) = defaultLog(log)

  def eventDebugLog(log: Logging.Debug) = defaultLog(log)

  def defaultLog(log: LogEvent) = {
    logs.append(log)
    println("============================")
    println(log.toString)
    println("============================")
  }

  

  def createActor(actorName: String) = {}
}

object TransformationFrontend {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val config =
      (if (args.nonEmpty) ConfigFactory.parseString(s"akka.remote.netty.tcp.port=${args(0)}")
      else ConfigFactory.empty).withFallback(
        ConfigFactory.parseString("akka.cluster.roles = [frontend]")).
        withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val frontend = system.actorOf(Props[TransformationFrontend], name = "frontend")

    import system.dispatcher
    implicit val timeout = Timeout(5 seconds)
    for (n <- 1 to 120) {
      (frontend ? TransformationJob("hello-" + n)) onSuccess {
        case result => println(result)
      }
      // wait a while until next request,
      // to avoid flooding the console with output
      Thread.sleep(2000)
    }
    system.shutdown()
  }
}

//#frontend
class TransformationFrontend extends Actor {

  var backends = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  override def receive = {

    case job: TransformationJob if backends.isEmpty =>
      sender ! JobFailed("Service unavailable, try again later", job)

    case job: TransformationJob =>
      jobCounter += 1
      backends(jobCounter % backends.size) forward job

    case BackendRegistration if !backends.contains(sender) =>
      context watch sender
      backends = backends :+ sender

    case Terminated(a) =>
      backends = backends.filterNot(_ == a)
  }
}
//#frontend

object TransformationBackend {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val config =
      (if (args.nonEmpty) ConfigFactory.parseString(s"akka.remote.netty.tcp.port=${args(0)}")
      else ConfigFactory.empty).withFallback(
        ConfigFactory.parseString("akka.cluster.roles = [backend]")).
        withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    system.actorOf(Props[TransformationBackend], name = "backend")
  }
}

//#backend
class TransformationBackend extends Actor {

  val cluster = Cluster(context.system)

  // subscribe to cluster changes, MemberUp
  // re-subscribe when restart
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case TransformationJob(text) => sender ! TransformationResult(text.toUpperCase)
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) => register(m)
  }

  def register(member: Member): Unit =
    if (member.hasRole("frontend"))
      context.actorSelection(RootActorPath(member.address) / "user" / "frontend") !
        BackendRegistration
}
//#backend