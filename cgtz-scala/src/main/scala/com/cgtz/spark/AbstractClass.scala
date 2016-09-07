package com.cgtz.spark

object AbstractClass {

}
trait Friend {
  val name: String
  def listen() = println("You frient " + name + " is listening")
}
class Human(val name: String) extends Friend
class Man(override val name: String) extends Human(name)
class Woman(override val name: String) extends Human(name)
class Animal
class Dog(val name: String) extends Animal with Friend {

}
trait Logger {
  def log(msg: String)
}
class ConLogger extends Logger with Cloneable {
  override def log(msg: String) = println(msg)
}
trait TraitLogger extends Logger {
  override def log(msg: String) {
    println("TraintLogger Log content is : " + msg)
  }
}
