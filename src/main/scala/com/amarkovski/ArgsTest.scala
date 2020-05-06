package com.amarkovski

import scala.annotation.tailrec
import scala.sys

object ArgsTest extends App{


  override def main(args: Array[String]) {
    val usage = """
    Usage: $jar-path $main-class
    """

    if (args.length == 0) {
      println(usage)
      sys.exit(2)

    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    @tailrec
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map
        case "--max-size" :: value :: tail =>
          nextOption(map ++ Map('maxsize -> value.toInt), tail)
        case "--min-size" :: value :: tail =>
          nextOption(map ++ Map('minsize -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(),arglist)
    //println(options.get)

    args.foreach(println)


  }

}
