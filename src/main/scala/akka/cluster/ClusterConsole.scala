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

/**
 * Starts the console for clusters
 * Extend this class to implement
 * custom behavoir or call it itself.
 */
class ClusterConsole extends Actor {

  //Set up the webservice that will display the console
  implicit val actSystem = context.system
  akka.io.IO(Http) ! Http.Bind(self, interface = "localhost", port = 8080)

  //set up class level variables to be used later
  val cluster = Cluster(context.system)
  var logs = new ListBuffer[LogEvent]()

  override def preStart(): Unit = {
    //register for events
    context.system.eventStream.subscribe(self, classOf[Logging.LogEvent])
    cluster.subscribe(self, classOf[ClusterDomainEvent])
  }
  override def postStop(): Unit =  {
    //de-register events
    context.system.eventStream.unsubscribe(self)
  }

  //Class used to pass information around
  class ClusterInformation (val isAvailable: Boolean, 
    val isSingletonCluster: Boolean, 
    val leader: Option[Address],
    val members: Seq[Member],
    val unreachableMembers: Seq[Member],
    val logs: Seq[LogEvent])

  //Display the information to the user
  def index(info: ClusterInformation) = HttpResponse(
    entity = HttpEntity(`text/html`,
      <html>
        <body>
          <h1>Cluster Status</h1>
          <ul>
            <li>Is available: { info.isAvailable }</li>
            <li>Leader: { info.leader.toString }</li>
            <li>Is singleton cluster: { info.isSingletonCluster }</li>
            <li>Members: {info.members.size}
              <ul>
                { formatToHtml("<li>", info.members, "</li>") }
              </ul>
            </li>
            <li> Unreachable members: { info.unreachableMembers.size }
              <ul>
                {formatToHtml("<li>", info.unreachableMembers, "</li>")}
              </ul>
            </li>
            <li>Logs: {info.logs.size}
              <ul>
                { formatToHtml("<li>", info.logs, "</li>") }
              </ul>
            </li>
          </ul>
        </body>
      </html>.toString()
    )
  )

  //format a basic Seq
  def formatToHtml(first: String, items: Seq[_], last: String) = {
    var output: String = ""
    items.foreach { item => output += first + item.toString + last }
    scala.xml.Unparsed(output)
  }

  def receive = {    
    case _: Http.Connected => sender ! Http.Register(self)

    case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
      sender ! index(new ClusterInformation(isClusterAvailable, 
        isSingletonCluster, 
        getLeader, 
        getMembers, 
        getUnreachableMembers, 
        logs.reverse.toSeq))

    case InitializeLogger(_) => sender ! LoggerInitialized

    case Error(cause, logSource, logClass, message) => eventErrorLog(new Logging.Error(cause, logSource, logClass, message))
    case Warning(logSource, logClass, message) => eventWarningLog(new Logging.Warning(logSource, logClass, message))
    case Info(logSource, logClass, message) => eventInfoLog(new Logging.Info(logSource, logClass, message))
    case Debug(logSource, logClass, message) => eventDebugLog(new Logging.Debug(logSource, logClass, message))
  }


  val clusterView = cluster.readView

  //Gets clusters information
  def getMembers = clusterView.members.toSeq
  def getUnreachableMembers = clusterView.unreachableMembers.toSeq
  def getLeader = clusterView.leader
  def isSingletonCluster = clusterView.isSingletonCluster
  def isClusterAvailable = clusterView.isAvailable

  //Handles logs
  def eventErrorLog(log: Logging.Error) = defaultLog(log)
  def eventWarningLog(log: Logging.Warning) = defaultLog(log)
  def eventInfoLog(log: Logging.Info) = defaultLog(log)
  def eventDebugLog(log: Logging.Debug) = defaultLog(log)

  //Default log handling
  def defaultLog(log: LogEvent) = logs.append(log)

  /**
   * Used to create actors based upon a string input.
   * Override this method in order to create the actors
   * NOTE: Not yet implemented
   */
  def createActor(actorName: String) = {}
}