package controllers

import java.io.File
import javax.inject.Inject

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import play.api.Play.current
import play.api._
import play.api.libs.json._
import play.api.libs.concurrent.Promise
import play.api.libs.iteratee.{Concurrent, Enumerator, Iteratee}
import play.api.libs.ws._
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

import com.ripple.data.fof._

class Application @Inject()(ws: WSClient) extends Controller {

  def index = Action {
    Ok("Server::Flow of funds!\n")
  }

  def wsFlowOfFunds = WebSocket.acceptWithActor[JsValue, JsValue] {
    request =>
      out => {
        Logger.info("wsWithActor, client connected")
        FlowOfFundsActor.props(out)
      }
  }

}
