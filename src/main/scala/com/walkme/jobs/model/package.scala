package com.walkme.jobs.model

/**
 * @author Serhii Zapalskyi (szap) / WorldTicket A/S
 * @version 03.01.2020
 */

package object model {

  trait Data {
    def timestamp: Long
    def email: String
  }

  trait Ip {
    def ip: String
  }

  trait Element {
    def element: String
  }

  case class View(timestamp: Long, email: String, ip: String) extends Data with Ip


  case class Click(timestamp: Long, element: String, email: String) extends Data with Element

  case class Log(timestamp: Long, action: String, hashedEmail: String, maskedIp: String)
}
