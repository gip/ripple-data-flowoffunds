package com.ripple.data.fof

import java.io._
import java.util.zip._

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import play.api.libs.json._
import scala.collection.JavaConverters._
import collection.immutable.TreeMap
import collection.mutable.TreeSet


object FlowOfFundsActor {
  def props(out: ActorRef) = Props(new FlowOfFundsActor(out))
}

class FlowOfFundsActor(out: ActorRef) extends Actor {
  val rp = PaymentsFromFile // new PaymentsFromFile("/Users/gilles/gip/ripple/output_GilesPayments.csv.gz", "/Users/gilles/gip/ripple/ripple_gateways.csv")
  def receive = {
    case msg0: JsValue =>
      //if (msg == "goodbye") self ! PoisonPill
      // { "command": "flowoffund", "source": "rABC", "start_date": "2015-01-01", "end_date": null, "min_value_usd": 0.0001 }
      val event = (msg0 \ "event").as[String]
      val msg = (msg0 \ "data").as[JsValue]
      val cmd = (msg \ "command").as[String]
      val src = (msg \ "source").as[String]
      val startD = (msg \ "start_date").as[String]
      val endD = Some((msg \ "end_date").as[String])
      val valUSD = Some((msg \ "min_value_usd").as[Double])
      val fof = rp.flowOfFunds(src, startD, endD, valUSD)
      out ! fof.toJSON()
  }
}


case class Payment(src: String, dest: String, date: String, curr: String, usd: Double, hash: String, toGateway: Boolean) extends Ordered[Payment] {
  import scala.math.Ordered.orderingToOrdered
  def compare(that: Payment): Int = this.hash compare that.hash
}

case class FlowOfFunds(src: String, 
	                   stardD:String, endD:Option[String], 
	                   pays:TreeSet[Payment], 
	                   order:collection.mutable.Map[String, (Int, Int, Double, Double, Double, Boolean)]) {

  val referenceUSD = order.toList.filter (t => t._2._1 == 0).head._2._5

  def findNodesByOrder(ord:Int) = order.toList.filter (t => t._2._1 == ord)

  def toJSON() = JsObject( List(("event", JsString("flowoffunds_done")), ("data", toJSON0())) )

  def toJSON0() = JsObject(
    List( ("size", JsNumber(pays.size)),
    	  ("referenceUSD", JsNumber(referenceUSD)),
    	  ("results", JsArray(order.toList.map(kv => JsObject(List( ("account", JsString(kv._1)), 
    	  	                                                        ("order", JsNumber(kv._2._1)),
    	  	                                                        ("valueUSDTotal", JsNumber(kv._2._3)),
    	  	                                                        ("valueUSDIn", JsNumber(kv._2._4)),
    	  	                                                        ("valueUSDDiff", JsNumber(kv._2._5)),
    	  	                                                        ("isGateway", JsBoolean(kv._2._6)) )
    	  	                                   )) ))
    	)
  )
}

abstract class RipplePayments {
  def findPayments(src: String, startD: String, endD: Option[String], usdMin: Option[Double]):Iterable[Payment] 
  def isGateway(addr: String):Boolean

  def flowOfFunds(src: String, startD:String, endD:Option[String], valUDS: Option[Double]):FlowOfFunds = {
   val sQueue  = new TreeSet[(Int, String, String, Double)]()                                 // Queue
   val mDone   = new java.util.TreeMap[String, String]()                                      // Using java TreeMap as no mutable TreeMap in Scala
   val mOrder  = new java.util.TreeMap[String, (Int, Int, Double, Double, Double, Boolean)]() // Node order
   val sResult = new TreeSet[Payment]()                                                       // Payments

   def doit():FlowOfFunds =
     sQueue.headOption match {
       case None => 
         println("Done")
         return new FlowOfFunds(src, startD, endD, sResult, mOrder.asScala)
       case Some((i, src0, startD0, usdIn)) =>
         println("Size: "+sQueue.size)
         sQueue -= ((i, src0, startD0, usdIn))
         val startD1 = mDone.get(src0)
         if(startD1==null || (startD1!=null && startD0 < startD1)) {
           //println("Searching payments"+(i, src0, startD0, endD))
           val pays = findPayments(src0, startD0, endD, valUDS)
           var usdOut = 0.0
           for (p <- pays) {
           	 // Get the payments
             val Payment(src2, dest2, date2, curr2, usd2, hash2, toG) = p
             val usdIn2 = math.min(usdIn, usd2)
             usdOut = usdOut + usd2
             // Update destination
             if(!mOrder.containsKey(dest2)) mOrder.put(dest2, (i+1, 1, usd2, usdIn2, 0.0, toG))
             else {
               val (i3, count, usd3, usdIn3, usdOut3, toG3) = mOrder.get(dest2)
               mOrder.put(dest2, (i3, count+1, usd3+usd2, usdIn2+usdIn3, usdOut3, toG3))
             }
             if(!toG) sQueue += ((i+1, dest2, date2, usdIn2))
           }
           sResult ++= pays
           mDone.put(src0, startD0)
           if(!mOrder.containsKey(src0)) { println("### Algorithm error") }
           else {
           	  val (i4, count4, usd4, usdIn4, usdOut4, toG4) = mOrder.get(src0)
           	  mOrder.put(src0, (i4, count4, usd4, usdIn4, usdOut4+usdOut, toG4))
           }

           doit()
         } else {
           doit()
         }
     }
    sQueue += ((0, src, startD, Double.MaxValue))
    mOrder.put(src, (0, 0, 0.0, Double.MaxValue, 0.0, false))
    doit()   
  }

}

object PaymentsFromFile extends RipplePayments {
  
  val paysFile = "/Users/gilles/gip/ripple/output_GilesPayments.csv.gz"
  val gatewaysFile = "/Users/gilles/gip/ripple/ripple_gateways.csv"

  var g = new TreeMap[String, String]()
  val gateLines = scala.io.Source.fromInputStream( new BufferedInputStream(new FileInputStream(gatewaysFile)) ).getLines
  for( l <- gateLines ) {
    val Array(addr, name) = l.split(",")
    println(addr, name)
    g = g + ((addr, name))
  }

  print("Loading payments ")
  var m = new TreeMap[(String, String, Double), (String, String, String)]()
  val paysLines = scala.io.Source.fromInputStream( new GZIPInputStream(new BufferedInputStream(new FileInputStream(paysFile))) ).getLines
  var z = 0
  for( l <- paysLines ) {
   z = z + 1
   if (z % 100000 == 0) { print(".") }
   val Array(hash, src, dest, c, i, v, usd, xrp, date) = l.split(",")
   m = m + ((src, date, usd.toDouble) -> (dest, hash, c+"@"+i))
  }

  println(" Loaded, ready to execute")

  def findPayments(src: String, startD: String, endD: Option[String], usdMin: Option[Double]):Iterable[Payment] = {
    val st = (src, startD, 0.0)
    val en = endD match {
      case Some(d) => (src, d, 0.0)
      case None => (src, "9999-99-99", 0.0)
    }
    val pl = m.range(st, en)
    usdMin match {
      case Some(usdVal) => (pl filter (_._1._3 >= usdVal)) map (p => Payment(src, p._2._1, p._1._2, p._2._3, p._1._3, p._2._2, isGateway(p._2._1)) )
      case None => pl map (p => Payment(src, p._2._1, p._1._2, p._2._3, p._1._3, p._2._2, isGateway(p._2._1)) )
    }
  }

  def isGateway(addr: String):Boolean = g.contains(addr)

}

