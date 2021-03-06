package sample

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
    val console = system.actorOf(Props[Manager], name = "console")
  }
}

class Manager extends ClusterConsole

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