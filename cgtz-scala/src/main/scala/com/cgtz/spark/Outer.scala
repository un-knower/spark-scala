package com.cgtz.spark
class Outer {
  class Inner{
    def f(){println("dddd")}
    class InnerMost{
      f()
    }
  }
  (new Inner).f()
}
package bobsroccketes{
  package navigation{
    private[bobsroccketes] class Navigator{
      protected[navigation] def useStarChart(){}
      class LegOfJourney{
        private[Navigator] val distance = 100
      }
      private[this] var speed = 200
    }
    package launch{
      import navigation._
      object Vehicle{
      }
    }   
  }
}