package com.cgtz.spark

import com.sun.xml.internal.ws.wsdl.writer.document.Import

/**
 * @author ${user.name}
 */
object Test {
  val xmax,ymax = 100
  val (ni,hao) = Pair(12,4)
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }
}
final case class Symbol private (name:String){
  override def toString:String="'"+name
  var myVar  = "Foo"
  val myVal = "Too"
}