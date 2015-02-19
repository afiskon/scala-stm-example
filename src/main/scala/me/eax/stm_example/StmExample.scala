package me.eax.stm_example

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.stm._
import scala.util._
import scala.concurrent.ExecutionContext.Implicits.global

object AccountStates {
  val totalAccounts = 10
  var states: Map[String, Ref[Int]] = {
      for (i <- 0 until totalAccounts)
      yield s"account$i" -> Ref(1000)
    }.toMap

  def randomAccount: String = s"account${Random.nextInt(totalAccounts)}"

  def transfer(fromAccount: String, toAccount: String, amount: Int)
              (implicit txn: InTxn) {
    states(fromAccount)() = states(fromAccount)() - amount
    states(toAccount)() = states(toAccount)() + amount
  }

  @tailrec
  def runTransactions(threadNumber: Int, transactionNumber: Int) {
    if(transactionNumber <= 0) return
    val fromAccount = randomAccount
    var toAccount = ""
    do toAccount = randomAccount while(fromAccount == toAccount)
    atomic { implicit txn =>
      val amount = Random.nextInt(states(fromAccount).get)
      println(
        s"[$threadNumber-$transactionNumber] Transferring $amount " +
        s"USD from $fromAccount to $toAccount...")
      transfer(fromAccount, toAccount, amount)
    }
    runTransactions(threadNumber, transactionNumber - 1)
  }
}

object StmExample extends App {
  val futuresSeq = {
    for(thrNum <- 1 to 10)
    yield {
      val f = Future { AccountStates.runTransactions(thrNum, 100) }
      f onFailure { case e => println(s"Future $thrNum failed: $e") }
      f
    }
  }

  val fResults = Future.sequence(futuresSeq)
  Await.ready(fResults, Duration.Inf)

  println("FINAL STATE:")
  val finalState = {
    atomic { implicit txn =>
      for((acc, balance) <- AccountStates.states)
      yield acc -> balance()
    }
  }

  for((acc, balance) <- finalState) {
    println(s"$acc -> $balance")
  }

  println(s"TOTAL: ${finalState.values.sum}")
}
