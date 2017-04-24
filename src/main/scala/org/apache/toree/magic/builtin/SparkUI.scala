package org.apache.toree.magic.builtin

import java.io.PrintStream

import org.apache.spark.SparkContext
import org.apache.toree.interpreter.{ExecuteError, Interpreter, Results}
import org.apache.toree.kernel.protocol.v5.MIMEType
import org.apache.toree.magic._
import org.apache.toree.magic.dependencies.{IncludeOutputStream, IncludeSparkContext}
import org.apache.toree.plugins.Plugin
import org.apache.toree.plugins.annotations.{Event, Init}
import org.apache.toree.utils.LogLike

class SparkUI extends LineMagic with IncludeSparkContext with IncludeOutputStream{

  private def printStream = new PrintStream(outputStream)

  @Event(name = "sparkui")
  override def execute(code: String): Unit = {
    val sc: SparkContext = sparkContext
    val uri = sc.uiWebUrl.get
    printStream.println(s"Wer ui: $uri.")
  }
}

class TestMagic extends LineMagic with IncludeSparkContext with IncludeOutputStream{

  private def printStream = new PrintStream(outputStream)

  @Event(name = "testmagic")
  override def execute(code: String): Unit = {
    printStream.println(s"Primer magic por Antonio.")
  }
}

class CustomCodeInjection extends Plugin with LogLike{

  @Init protected def init(newInterpreter: Interpreter) = {
    _interpreter = newInterpreter
  }

  @Event(name = "newOutputStream")
  protected def newOutputStream(): Unit ={
    val (result, message) = kernelInterpreter.interpret("val a = 5")
    result match {
      case Results.Success =>
        logger.info("Antonio - Ok custom code injection")
      case Results.Aborted =>
        logger.error("Antonio - Noooooooo custom code injection")
      case Results.Error =>
        logger.error("Antonio - Noooooooo custom code injection")
      case Results.Incomplete =>
        logger.error("Antonio - Noooooooo custom code injection")

    }
  }


  private var _interpreter: Interpreter = _
  def kernelInterpreter: Interpreter = _interpreter

}