package local
 
import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
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

case class start_workers(no_of_zeros:Integer)
case class contact_remote(remote_ip:String,no_of_zeros:Integer)
case class start_mining(no_of_zeros: Integer)
case class mining_done(inputsprocessed:Integer)
case class coins_mined(mined_bitcoins:ArrayBuffer[String])
 
 
class Worker extends Actor {
   
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
        var flag:Int= 0
       
        while(zeros > 0)
        {
          if (hashstring(ct) != '0') 
          {
            flag=1
          }
          ct =ct+1
          zeros= zeros -1
        }
 
        if(flag == 0) {
          return 1
        }
        else {
          return 0
        }
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
              
          }
          sender ! mining_done(count)
          
      }
  }
}
 

class Master extends Actor {

  var total_mined_bitcoins:ArrayBuffer[String]= ArrayBuffer[String]()
  var workernumber:Int=0
  var total_count:Int=0
  var worker_count:Int=0
  var zeros:Integer=0
  var noofbitcoins:Int = 0
  var local_done:Boolean = false
  var remote_done:Boolean = false
  var cancel:Cancellable = null
 
  def receive = {
   
    case start_workers(no_of_zeros:Integer) => {
        zeros=no_of_zeros
        worker_count+=12
        println("Master is live")
        val worker =context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfInstances=12)))
        for (n <- 1 to 12)
            worker ! start_mining(zeros)
    }
   
    case coins_mined(mined_bitcoins:ArrayBuffer[String]) => {
        total_mined_bitcoins++=mined_bitcoins
    }

    case mining_done(count:Integer) => {

        workernumber+=1
        total_count+=count
        if(workernumber == worker_count) {
            local_done = true
            println("Worker actor count : "+worker_count)
            println("Total number of strings processed : "+total_count)
            total_mined_bitcoins=total_mined_bitcoins.distinct
            for(i<- 0 until total_mined_bitcoins.length )
                println((i+1)+" " + total_mined_bitcoins(i))
            
            println("Number of locally mined bitcoins : "+total_mined_bitcoins.length )
            if (local_done && remote_done)
                context.system.shutdown()
        }
    }
    
    case remote_work_done(remotely_mined_bitcoins:ArrayBuffer[String]) => {
        remote_done = true
        for(i<- 0 until remotely_mined_bitcoins.length )
            println((i+1)+" " + remotely_mined_bitcoins(i))
        println("Number of remotely mined bitcoins: "+remotely_mined_bitcoins.length )
        if (local_done && remote_done)
            context.system.shutdown()
    }

    case contact_remote(remote_ip:String,no_of_zeros:Integer) => {
        val remoteActor = context.actorSelection("akka.tcp://RemoteSystem@"+remote_ip+":5150/user/remote")
        cancel = context.system.scheduler.schedule(0.seconds, 1.seconds) {
            remoteActor ! spawn_remote(no_of_zeros)
        }
    }

    case msg:String => {
        cancel.cancel()
        println(s"Received msg from remote: $msg")
    }
  }
}
   
object btcmining extends App {
 
    var remote_ip:String = null
    var no_of_zeros:Int = 4

    args.length match {
      case 2 => {
        no_of_zeros = args(0).toInt
        remote_ip = args(1)
      }
      case 1 => {
        no_of_zeros = args(0).toInt
      }
      case 0 => {
        println(s"No args: Mining for default $no_of_zeros no.of zeroes")
      }
    }
    
    val configFile = getClass.getClassLoader.getResource("local_application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    val system = ActorSystem("BTCActorSystem",config)
    val master = system.actorOf(Props[Master],name="master")
    
    master ! start_workers(no_of_zeros)
    
    if(remote_ip != null)
      master ! contact_remote(remote_ip,no_of_zeros)
    
    println("Bitcoin App is live!")
    
}