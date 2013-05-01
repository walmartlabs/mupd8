import scala.actors._
import scala.actors.Actor._
import grizzled.slf4j.Logging

class TimerActor(val timeout: Long, val who: Actor, val reply: Any) extends Actor with Logging {
  def act {
    self.reactWithin(timeout) {
      case TIMEOUT => who ! reply
      case Exit(from: AbstractActor, reason: AnyRef) =>
        info("Timer exit; request by -  " + from + ", reason - " + reason)
        Actor.exit()
    }
  }
}
