package remote
 
import java.security.MessageDigest
 
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import java.io.File
import akka.actor._
import akka.actor.Actor
import akka.actor.ActorDSL._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory
import common._
 

case class start_mining(no_of_zeros: Integer)
case class mining_done(inputsprocessed:Integer)
case class coins_mined(mined_bitcoins:ArrayBuffer[String])

class RemoteWorker extends Actor {
   
  def string_generator() :String = {
      var randomval=new Random()
      var message=new String()
      var number=randomval.nextInt(25) + 5
      for(i <- 1 to number)
      {
          var randomnumber=randomval.nextInt(93)+33
          message+=randomnumber.toChar
      }
      return message
  }
   
  def hashing(message:String):String = {
      var flag:Integer =0
      var ct=0
       
      //Hashing
      var message_digest= MessageDigest.getInstance("SHA-256")
      message_digest.update(message.getBytes())
      var hash_value=message_digest.digest()
      var hexstring= new StringBuffer()
      for ( i <- 0 to (hash_value.length-1)) {
          var hex = Integer.toHexString(0xff & hash_value(i))
          if(hex.length() == 1) hexstring.append('0')
            hexstring.append(hex)
      }
      var hashstring=hexstring.toString()
      return hashstring
  }
   
  def hash_checker(hashstring:String,zeroes:Integer) :Int = {  
      var zeros=zeroes
      var ct:Int=0
      var flag:Boolean= false
       
      while(zeros > 0)
      {
          if (hashstring(ct) != '0') flag= true
          
          ct =ct+1
          zeros= zeros -1
      }
 
      if(flag) return 1
      else return 0       
  }
 
  def receive = {
  
      case start_mining(no_of_zeros:Integer) => {
          var mined_bitcoins:ArrayBuffer[String]=  ArrayBuffer[String]()
          var count:Integer=0
          val start_time=System.currentTimeMillis()
     
          while(System.currentTimeMillis()-start_time <30000){
              var s:String = "skandaswami;"+string_generator()
              var hash_value:String = hashing(s)
              if(hash_checker(hash_value,no_of_zeros)==1)
                  mined_bitcoins+=hash_value
              count+=1
              if(count==100000)
                  sender ! coins_mined(mined_bitcoins)
              sender ! mining_done(count)
          }
          
      }
  }
}


class RemoteActor extends Actor {

  var total_mined_bitcoins:ArrayBuffer[String]= ArrayBuffer[String]()
  var workernumber:Integer=0
  var total_count:Integer=0
  var worker_count:Integer=0
  var zeros:Integer=0
  var noofbitcoins:Integer = 0
  var LocalMaster: ActorRef = _

  override def receive: Receive = {
    case spawn_remote(no_of_zeros: Integer) => {
        LocalMaster = sender
        println(s"Work request received from Local- zeros $no_of_zeros")
        sender ! "Ack: Work request received"
        zeros=no_of_zeros
        worker_count+=8
        println("Master is live")
        val worker =context.actorOf(Props[RemoteWorker].withRouter(RoundRobinRouter(nrOfInstances=8)))
        for (n <- 1 to 8)
          worker ! start_mining(zeros)
    }

    case coins_mined(mined_bitcoins:ArrayBuffer[String]) => {
        total_mined_bitcoins++=mined_bitcoins
    }
    
    case mining_done(count:Integer) => {
        workernumber+=1
        total_count+=count
        if(workernumber == worker_count)
        {
           println("Remote Worker actor count : "+worker_count)
           println("Total number of strings processed remote: "+total_count)
           total_mined_bitcoins=total_mined_bitcoins.distinct
           println("Number of bitcoins found by remote: "+total_mined_bitcoins.length )
           LocalMaster ! remote_work_done(total_mined_bitcoins)

           context.system.shutdown()
        }
    }

    case _ => println("Received unknown message")

  }
}

 
object RemoteActor{
  def main(args: Array[String]) {
    
      val configFile = getClass.getClassLoader.getResource("remote_application.conf").getFile
      val config = ConfigFactory.parseFile(new File(configFile))
      val system = ActorSystem("RemoteSystem" , config)
      val remote = system.actorOf(Props[RemoteActor], name="remote")

      println("Remote Mining System is Ready!")
  }
}