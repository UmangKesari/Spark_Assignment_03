package com.knoldus

import java.sql.{Connection, DriverManager}

import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.Future

class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart() = {
    // Start the thread that receives data over a connection
    Future{
      val logger =Logger.getLogger(this.getClass)
      logger.info("")
      val driver ="org.postgresql.Driver"
      val url = "jdbc:postgresql://localhost:5432/saprkDB"
      val username ="postgres"
      val password ="knoldus123"
      var conn :Connection =null
      try{
        conn = DriverManager.getConnection(url,username,password)
      }
      catch{
        case exe : Exception => throw new Exception("Error in connnecting database")

      }

      val statement = conn.createStatement();
      val query = statement.executeQuery("SELECT * FROM sparkDB")
      while(query.next())
        {
          store(query.getString("id"))
          logger.info(query.getString("id"))
        }
      conn.close()
      restart("restarting stream to get more data")
    }
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }
  /*private def receive() ={

      var conn: Connection = null
      var userInput: String = null
  }*/
}
