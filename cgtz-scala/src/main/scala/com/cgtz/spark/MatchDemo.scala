package com.cgtz.spark

object MatchDemo {
  def main(args: Array[String]): Unit = {
    println(matchTest("two"))
    println(matchTest("test"))
    println(matchTest(1))
    println(matchTest(6))

    ///////////////case class/////////
    val alice = new Person("alice", 23)
    val bo = new Person("bo", 32)
    val lie = new Person("lie", 43)
    for (person <- List(alice, bo, lie)) {
      person match {
        case Person("alice", 23) => println("aaaaaa")
        case Person("bo", 32)    => println("bbbbbbbbb")
        //case Person("lie", 43)   => println("llllllllll")
        case Person(name, age)   => println("Age: " + age + " year, name: " + name + "?")
      }
    }
  }
  def matchTest(x: Any): Any = x match {
    case 1      => "one"
    case "two"  => 2
    case y: Int => "scala.Int"
    case _      => "many"
  }

  case class Person(name: String, age: Int)
}