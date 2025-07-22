package org.sunbird.job.contentActivity.domain

import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)  extends JobRequest(eventMap, partition, offset) {
    
    /**
      * contains the event ID of the event
      *
      * @return
      */
    def eventId: String = readOrDefault[String]("mid", "")

    /**
      * contain operation type of the event
      * operation type can be CREATE or UPDATE
      * This is used to identify the type of event
      * and to process the event accordingly.
      * @return
      */ 
    def operationType: String = readOrDefault[String]("operationType", "")

    /**
      * contains do_id of a content
      * @return
      */
    def contentId: String = readOrDefault[String]("nodeUniqueId", "")

    /**
      * contains the created on date of the content
      * @return
      */
    def createdOn: String = readOrDefault[String]("transactionData.createdOn.nv", "")

    /**
      * contains the update date on date of the content
      * @return
      */
    def lastUpdatedOn: String = readOrDefault[String]("transactionData.lastUpdatedOn.nv", "")

    /**
      * contains the transaction data of the event
      * transaction data contains the details of the content
      * like created on date, last updated on date, status, channel, etc.
      * transaction data is a map of key value pairs
      * where key is the field name and value is the field value
      * @return
      */
    def transactionData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("transactionData", Map[String, AnyRef]())

    /**
      * contains the last status changed on date of the content
      *
      * @return
      */
    def lastStatusChangedOn: String = readOrDefault[String]("transactionData.lastStatusChangedOn.nv", "")

    /**
      * contains the channel or org ID of the content
      *
      * @return
      */
    def channel: String = readOrDefault[String]("transactionData.channel.nv", "")

    /**
      * contains status information of the content
      *
      * @return
      */
    def status: String = readOrDefault[String]("transactionData.status.nv", "")

    /**
      * contains creator information of the content
      *
      * @return
      */
    def userId: String = readOrDefault[String]("userId", "")

    /**
      * contains creator information of the content
      *
      * @return
      */
    def createdBy: String = readOrDefault[String]("transactionData.createdBy.nv", "")

    def toMap: Map[String, Any] = {
        Map(
            "eventId" -> eventId,
            "operationType" -> operationType,
            "contentId" -> contentId,
            "createdOn" -> createdOn,
            "lastUpdatedOn" -> lastUpdatedOn,
            "transactionData" -> transactionData,
            "lastStatusChangedOn" -> lastStatusChangedOn,
            "channel" -> channel,
            "status" -> status,
            "userId" -> userId,
            "createdBy" -> createdBy
        )
    }
}