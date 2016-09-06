package com.cgtz.spark
// 私有构造方法
class Marker private (val color: String) {
  println("创建" + this)
  override def toString(): String = "颜色标记：" + color
}
object Marker {

  private val markers = Map(
    "red" -> new Marker("red"),
    "blue" -> new Marker("blue"),
    "green" -> new Marker("green"))

  def apply(color: String) = {
    if (markers.contains(color)) markers(color) else null
  }

  def getMarker(color: String) = {
    if (markers.contains(color)) markers(color) else null
  }
  def main(args: Array[String]) {
    //println(Marker("red"))
    // 单例函数调用，省略了.(点)符号  
    //println(Marker getMarker "blue")

    //    val s = new Student("liuxing",12,"riben","京东大学")
    //    println(s.name)
    //    println(s.age)
    //    println(s.grade)
    
    val fam = new Family("liuxing","huping")
    fam.info()
  }
}

/*******************///////////////////////////***********************/
/**
 * 对于Scala类，主构造器的参数放置在类名后，由括号括起来。
 * 且对于主构造器中var、val、private 等标注的参数，
 * 都会成为类的对应字段，并生成对应的默认getter、setter方法。
 * 
 * 对于主构造器中的未用var、val标注的参数，
 * 如果在类的任何一个方法用用到该参数，
 * 该参数将会转换为类的字段，否则不会，
 * 如Student类的address属性。
 * 
 * 主构造器的函数体，是类中除了方法定义以外的其他语句，
 * 如在Student类的主构造器中，包含grade属性的初始化和prinln这两行语句。
 */
class Student(val name:String,var age:Int = 0,address:String ="" ,private var school:String=""){
  var grade = if (age > 7) age - 7 else 0
  println(" I'm in main constructor.  ")
  
  def info() = " name is " + name + ", age is " + age + ", address is " + address
}
/**
 * 辅助构造器
 * 辅助构造器通过this来定义，且必须首先调用主构造器或者其他已经定义的辅助构造器。
 * 【注：辅助构造器的参数前不能添加val、var标志，否则会报错。】
 */
class PersonAuxClass(val name:String){
  var age = 0
  var sex='f'
  println("main constructor...")
  
  def this(name:String,age:Int){
    this(name)  //调用主构造器
    this.age = age  //使用this关键字
    println(" auxiliary constructor1 ")
  }
  
  def this(name:String,age:Int,sex:Char){
    this(name, age)
    this.sex = sex
    println(" auxiliary constructor2 ")
  }
}
/**
 * 私有主构造器
 * 私有构造器通过在类名后用private关键字标注主构造器参数来标明。
 * 此时，可以通过辅助构造器来创建该类的对象。
 */
class PersonPivate private(val name:String){
  var age = 1
  def this(name:String,age:Int){
    this(name)
    this.age = age
  }
}
/**
 * 嵌套类
 */
class Family(val h_name:String,val w_name:String){
  class Husband(var name:String){
    println("i am a husband")
  }
  
  class Wife(var name:String){
    println("i am a wife")
  }
  
  var husband = new Husband(h_name)
  var wife = new Wife(w_name)
  def info(){
    println( "husband: "+husband.name+", wife:"+wife.name )
  }
}
/**
 * Scala中没有静态方法和静态字段，但是可以用object语法来实现类似的功能。对象定义了某个类的单个实例。
 * Scala的object中可以用来实现类似的功能，用来存放工具函数或常量等。如，
 * 使用object中的常量或方法，通过object名直接调用，
 * 对象构造器在对象第一次被使用时调用（如果某对象一直未被使用，那么其构造器也不会被调用）。
 */
object Sequenec{
  private var next_num = 0
  val threshold = 100
  
  def getSequence()={
    next_num += 1
    next_num
  }
}
/**
 * 伴生对象
 *　可以将在Java类中定义的静态常量、方法等放置到Scala的类的伴生对象中。
 * 伴生对象与类同名，且必须放置在同一源文件中。类可以访问伴生对象私有特性，
 * 但是必须通过 伴生对象.属性名 或 伴生对象.方法 调用。
 *　伴生对象是类的一个特殊实例
 * 
 * 如在类Counter访问其伴生对象的私有方法getCount，
 * 必须通过 Counter.getCount() 的方式调用。
 */
class Counter {
	def getTotalCounter()= Counter.getCount
}
object Counter{
  private var cnt = 0
  private def getCount()=cnt
}
/**
 * 对象可以继承或扩展多个特质
 */
abstract class AbsPerson(var name:String,val age:Int){
  def info():Unit
}
object XiaoMing extends AbsPerson("XiaoMing",5){
  def info(){
    println(" name is "+name+", age is "+age)
  }
}
/**
 * apply方法
 * 当遇到 object(参数1, 参数2,....,参数n)的形式的调用时，
 * apply方法便会被调用。
 * */
/**
 * 枚举
 * Scala并没有定义枚举类型，但是可以通过定义扩展Enumeration的对象，
 * 并用Value方法初始化枚举类中的所有可选值，提供枚举。
 * 上述实例中的val Red, Yellow, Green = Value语句，相当于
 * val Red = Value
 * val Yellow = Value
 * val Green = Value
 */
object TrafficeLight extends Enumeration{
  //val Red,Yellow,Green = Value
  
  //用Value方法初始化枚举类变量时，Value方法会返回内部类的新实例，
  //且该内部类也叫Value。另外，在调用Value方法时，也可传入ID、名称两参数。
  //如果未指定ID，默认从零开始，后面参数的ID是前一参数ID值加1。
  //如果未指定名称，默认与属性字段同名。
  val Red = Value(1, "Stop")
  val Yellow = Value("Wait")    //可以单独传名称
  val Green = Value(4) //可以单独传ID
}