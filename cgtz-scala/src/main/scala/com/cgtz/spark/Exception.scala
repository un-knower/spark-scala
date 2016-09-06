package com.cgtz.spark

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

object Exception {
  def main(args: Array[String]): Unit = {
    exOne
  }
  def exOne():Unit={
    try{
      val f = new FileReader("input.txt")
    }catch {
      case ex:FileNotFoundException=>{
        println("FileNotFoundException")
      }
      case ex:IOException=>{
        println("IO Exception")
      }
    }finally{
      println("Exiting finally...")
    }
  }
}