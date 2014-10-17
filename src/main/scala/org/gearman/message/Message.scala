/*
* Copyright (c) 2014 by its authors. Some rights reserved.
* See the project homepage at
*
* https://github.com/ochinchina/gearman-scala
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.gearman.message

import java.io.{InputStream,
			OutputStream,
			DataInputStream,
			DataOutputStream,
			ByteArrayInputStream,
			ByteArrayOutputStream,
			IOException}

/**
 * Job priority
 */ 
object JobPriority extends Enumeration {
	type JobPriority = Value
	val Low, Normal, High = Value
}

/**
 * represents a message concept in the Gearman protocol
 */ 
trait Message {
	/**
	 * serialize the message to a DataOutputStream
	 * 
	 * @param out the output stream this message will be serialized to 	 	 
	 */	 	
	def writeTo( out: DataOutputStream  )
}

/**
 * administration request from client
 */ 
case class AdminRequest( command: String, args: List[ String ] ) extends Message {
	def writeTo( out: DataOutputStream ) {
		out.write( command.getBytes( "UTF-8") )
		args.foreach {
			(s: String ) =>
				out.write( ' ' )
				out.write( s.getBytes( "UTF-8" ) ) 
		}
				
	}
}

object AdminRequest {
	def parse( line: String ) = {
		val words = line.split( "\\s+")
		if( words.length <= 0 ) {
			null
		} else {
			val command = words(0)
			var args = List[String]()
			for( i <- 1 until words.length ) {
				args =args :+ words(i) 
			}
			new AdminRequest( command, args)
		}
	}
}

case class AdminResponse( lines: List[ String ] ) extends Message {
	def writeTo( out: DataOutputStream ) {
		lines.foreach { (s:String)=>
			out.write( s.getBytes( "UTF-8") )
			out.write( '\n')
		}
	}
} 

abstract class BinMessage extends Message {
	def getType: Int
}


object Message {	
	val ReqMagicCode: Int = new DataInputStream( new ByteArrayInputStream("\0REQ".getBytes("UTF-8") ) ).readInt
	val ResMagicCode: Int = new DataInputStream( new ByteArrayInputStream("\0RES".getBytes("UTF-8") ) ).readInt
	val CAN_DO = 1
	val CANT_DO = 2
	val RESET_ABILITIES = 3
	val PRE_SLEEP = 4
	val NOOP = 6	 
	val SUBMIT_JOB = 7
	val JOB_CREATED = 8
	val GRAB_JOB = 9
	val NO_JOB = 10
	val JOB_ASSIGN = 11
	val WORK_STATUS = 12
	val WORK_COMPLETE = 13
	val WORK_FAIL  = 14
	val GET_STATUS = 15
	val ECHO_REQ = 16
	val ECHO_RES = 17
	val SUBMIT_JOB_BG = 18
	val ERROR = 19
	val STATUS_RES = 20
	val SUBMIT_JOB_HIGH = 21
	val SET_CLIENT_ID = 22
	val CAN_DO_TIMEOUT = 23
	val ALL_YOURS = 24
	val WORK_EXCEPTION = 25
	val OPTION_REQ = 26
	val OPTION_RES = 27
	val WORK_DATA = 28
	val WORK_WARNING = 29
	val GRAB_JOB_UNIQ = 30
	val JOB_ASSIGN_UNIQ = 31
	val SUBMIT_JOB_HIGH_BG = 32
	val SUBMIT_JOB_LOW = 33
	val SUBMIT_JOB_LOW_BG = 34
	val SUBMIT_JOB_SCHED = 35
	val SUBMIT_JOB_EPOCH = 36
	
	
	
	def readFrom( in: DataInputStream ) = {
		val magicCode = in.readInt
		val packetType = in.readInt
		val packetLen = in.readInt
		var msg: Message = null
		
		packetType match {
			case CAN_DO => msg = new CanDo( readString( packetLen, in ) )
			case CANT_DO => msg = new CantDo( readString( packetLen, in ) )
			case RESET_ABILITIES => msg = new ResetAbilities
			case PRE_SLEEP => msg = new PreSleep
			case NOOP => msg = new Noop
			case SUBMIT_JOB => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new SubmitJob( params(0), params(1), params(2) ) 
			}
			case JOB_CREATED => msg = new JobCreated( readString( packetLen, in ) )
			case GRAB_JOB => msg = new GrabJob
			case NO_JOB => msg = new NoJob
			case JOB_ASSIGN => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new JobAssign( params(0), params(1), params(2) )
			}
			case WORK_STATUS => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				if( magicCode == ReqMagicCode ) 
					msg = new WorkStatusReq( params(0), params(1).toInt, params(2).toInt )
				else msg = new WorkStatusRes( params(0), params(1).toInt, params(2).toInt ) 
			}
			case WORK_COMPLETE  => {
				val params = parseParams( readBytes( packetLen, in ), 2 )
				if( magicCode == ReqMagicCode )
					msg = new WorkCompleteReq( params(0), params(1) )
				else msg = new WorkCompleteRes( params(0), params(1) )
				
			}
			case WORK_FAIL => if( packetType == ReqMagicCode ) msg = new WorkFailReq( readString( packetLen, in ) ) else msg = WorkFailRes( readString( packetLen, in ) ) 
			case GET_STATUS => msg = new GetStatus( readString( packetLen, in ) )	
			case ECHO_REQ => msg = new EchoReq( readString( packetLen, in ) )
			case ECHO_RES => msg = new EchoRes( readString( packetLen, in ) )
			case SUBMIT_JOB_BG => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new SubmitJobBg( params(0), params(1), params(2) )
			}
			case ERROR => {
				val params = parseParams( readBytes( packetLen, in ), 2 )
				msg = new Error( params(0), params(1) )
			}
			case STATUS_RES => {
				val params = parseParams( readBytes( packetLen, in ), 5 )
				msg = new StatusRes( params(0), params(1).toInt != 0, params(2).toInt != 0, params(3).toInt, params(4).toInt )
			}
			case SUBMIT_JOB_HIGH => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new SubmitJobHigh( params(0), params(1), params(2) )
			}
			case SET_CLIENT_ID => msg = new SetClientId( readString( packetLen, in ) )
			case CAN_DO_TIMEOUT => {
				val params = parseParams( readBytes( packetLen, in ), 2 )
				msg = new CanDoTimeout( params(0), params(1).toInt )
			}
			case ALL_YOURS => msg = new AllYours
			case WORK_EXCEPTION =>  {
				val params = parseParams( readBytes( packetLen, in ), 2 )
				if( magicCode == ReqMagicCode ) 
					msg = new WorkExceptionReq( params(0), params(1) )
				else msg = new WorkExceptionRes( params(0), params(1) ) 
			}
			case OPTION_REQ => msg = new OptionReq( readString( packetLen, in ) )
			case OPTION_RES => msg = new OptionRes( readString( packetLen, in ) )
			case WORK_DATA => {
				val params = parseParams( readBytes( packetLen, in ), 2 )
				if( magicCode == ReqMagicCode )
					msg = new WorkDataReq( params(0), params(1) )
				else msg = new WorkDataRes( params(0), params(1) ) 
			}
			case WORK_WARNING => {
				val params = parseParams( readBytes( packetLen, in ), 2 )
				if( magicCode == ReqMagicCode )
					msg = new WorkWarningReq( params(0), params(1) )
				else msg = new WorkWarningRes( params(0), params(1) ) 
			}
			case GRAB_JOB_UNIQ => msg = new GrabJobUniq
			case JOB_ASSIGN_UNIQ => {
				val params = parseParams( readBytes( packetLen, in ), 4 )
				msg = new JobAssignUniq( params(0), params(1), params(2), params(3) )
			}
			case SUBMIT_JOB_HIGH_BG => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new SubmitJobHighBg( params(0), params(1), params(2) )
			}
			case SUBMIT_JOB_LOW => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new SubmitJobLow( params(0), params(1), params(2) )
			}
			case SUBMIT_JOB_LOW_BG => {
				val params = parseParams( readBytes( packetLen, in ), 3 )
				msg = new SubmitJobLowBg( params(0), params(1), params(2) )
			}
			case SUBMIT_JOB_SCHED => {
				val params = parseParams( readBytes( packetLen, in ), 8 )
				msg = new SubmitJobSched( params(0), params(1), params(2).toInt, params(3).toInt, params(4).toInt, params(5).toInt, params(6).toInt, params(7) ) 
			}
			case SUBMIT_JOB_EPOCH => {
				val params = parseParams( readBytes( packetLen, in ), 8 )
				msg = new SubmitJobEpoch( params(0), params(1), params(2).toLong, params(3) ) 
			}
		}
		
		msg
	}
	
	def readBytes( n: Int, in: InputStream ) = {
		import Array._
		
		val b = ofDim[Byte](n)
		var pos = 0
		
		while( pos < n ) {
			val i = in.read( b, pos, n - pos )
			if( i > 0 ) pos += i else throw new IOException( "no enough data to read") 
		}
		
		b		
	}
	
	def readString( n: Int, in: InputStream ) = new String( readBytes( n, in ), "UTF-8" )
	
	def parseParams( data: Array[ Byte ], n: Int ) = {
		import Array._
		
		val params = ofDim[String](n)
		var i = 0
		var j = 0
		
		for( k <- 0 until ( n - 1 ) ) {
			while( data(i) != 0 && i < data.length ) i += 1
			
			if( i < data.length ) {			
				params(k) = new String( data.slice( j, i ), "UTF-8" )
				j = i + 1
				i = j
			} else throw new RuntimeException( "Fail to parse " + n + " parameters")
		}
		
		params( n - 1 ) = new String( data.takeRight( data.length - j ), "UTF-8" )
		
		params
	}
}

trait BinRequest extends BinMessage {
	def writeTo( out: DataOutputStream  ) = {
		import Message._
		
		out.writeInt( ReqMagicCode )
		out.writeInt( getType )
		val bodyOut = new ByteArrayOutputStream
		writeBody( new DataOutputStream( bodyOut ) )
		val bodyBytes = bodyOut.toByteArray
		out.writeInt( bodyBytes.length )
		out.write( bodyBytes )
	}
	
	protected def writeBody( out: DataOutputStream )
}

trait BinResponse extends BinMessage {
	def writeTo( out: DataOutputStream  ) {
		import Message._
		
		out.writeInt( ResMagicCode )
		out.writeInt( getType )
		
		val bodyOut = new ByteArrayOutputStream
		writeBody( new DataOutputStream( bodyOut ) )
		val bodyBytes = bodyOut.toByteArray
		out.writeInt( bodyBytes.length )
		out.write( bodyBytes )
	}
	
	protected def writeBody( out: DataOutputStream )
}

case class CanDo( funcName: String ) extends BinRequest {
	def getType= Message.CAN_DO
	override protected def writeBody( out: DataOutputStream ) {
		out.write( funcName.getBytes("UTF-8") )
	}
}

case class CantDo( funcName: String ) extends BinRequest {
	def getType=Message.CANT_DO
	override protected def writeBody( out: DataOutputStream ) {
		out.write( funcName.getBytes("UTF-8") )
	}
}

case class ResetAbilities() extends BinRequest {
	def getType = Message.RESET_ABILITIES
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class PreSleep() extends BinRequest {
	def getType = Message.PRE_SLEEP
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class Noop() extends BinResponse {
	def getType = Message.NOOP
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class SubmitJob( funcName: String, uniqueId: String, data: String ) extends BinRequest {
	def getType = Message.SUBMIT_JOB
	override protected def writeBody( out: DataOutputStream ) {
		out.write( funcName.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( uniqueId.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class JobCreated( jobHandle: String ) extends BinResponse {
	def getType = Message.JOB_CREATED
	override protected def writeBody( out: DataOutputStream ) {
		out write jobHandle.getBytes("UTF-8")
	}
}
 

case class GrabJob() extends BinRequest {
	def getType = Message.GRAB_JOB
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class NoJob() extends BinResponse {
	def getType = Message.NO_JOB 
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class JobAssign( jobHandle: String, funcName: String, data: String ) extends BinResponse {
	def getType = Message.JOB_ASSIGN
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( funcName.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class WorkStatusReq(jobHandle: String, percentCompleteNumerator: Int, percentCompleteDenominator:Int ) extends BinRequest {
	def getType = Message.WORK_STATUS
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( percentCompleteNumerator.toString.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( percentCompleteDenominator.toString.getBytes( "UTF-8") )
	}
}

case class WorkStatusRes(jobHandle: String, percentCompleteNumerator: Int, percentCompleteDenominator:Int) extends BinResponse {
	def getType = Message.WORK_STATUS
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( percentCompleteNumerator.toString.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( percentCompleteDenominator.toString.getBytes( "UTF-8") )
	}
}

case class WorkCompleteReq(jobHandle: String, data: String) extends BinRequest {
	def getType = Message.WORK_COMPLETE
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}


case class WorkCompleteRes(jobHandle: String, data: String) extends BinResponse {
	def getType = Message.WORK_COMPLETE
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class WorkFailReq(jobHandle: String) extends BinRequest {
	def getType = Message.WORK_FAIL
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
	}
}


case class WorkFailRes(jobHandle: String) extends BinResponse {
	def getType = Message.WORK_FAIL
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
	}
}

case class GetStatus( jobHandle: String ) extends BinRequest {
	def getType = Message.GET_STATUS
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
	}
}

case class EchoReq( data: String ) extends BinRequest {

	def getType = Message.ECHO_REQ
	
	override protected def writeBody( out: DataOutputStream ) {
		out.write( data.getBytes("UTF-8") )
	}
}

case class EchoRes( data: String ) extends BinResponse {
	def getType = Message.ECHO_RES
	override protected def writeBody( out: DataOutputStream ) {
		out.write( data.getBytes("UTF-8") )
	}
}


case class SubmitJobBg(funcName: String, uniqueId: String, data: String) extends BinRequest {
	def getType = Message.SUBMIT_JOB_BG
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}

case class Error( code: String, text: String) extends BinResponse {
	def getType = Message.ERROR
	override protected def writeBody( out: DataOutputStream ) {
		out write code.getBytes( "UTF-8")
		out write 0
		out write text.getBytes( "UTF-8")
	}
}

case class StatusRes( jobHandle: String, knownStatus: Boolean, runningStatus: Boolean, percentCompleteNumerator: Int, percentCompleteDenominator:Int ) extends BinResponse {
	def getType = Message.STATUS_RES
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		if( knownStatus ) out.write( "1".getBytes("UTF-8") ) else out.write( "0".getBytes("UTF-8") )
		out.write( 0 )
		if( runningStatus ) out.write( "1".getBytes("UTF-8") ) else out.write( "0".getBytes("UTF-8") )
		out.write( 0 )
		out.write( percentCompleteNumerator.toString.getBytes("UTF-8") )
		out.write( 0 )
		out.write( percentCompleteDenominator.toString.getBytes("UTF-8") )
	}
}

case class SubmitJobHigh(funcName: String, uniqueId: String, data: String) extends BinRequest {
	def getType = Message.SUBMIT_JOB_HIGH
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}

case class SetClientId( workerId: String ) extends BinRequest {
	def getType = Message.SET_CLIENT_ID
	override protected def writeBody( out: DataOutputStream ) {
		out.write( workerId.getBytes( "UTF-8") )
	}
}

case class CanDoTimeout(funcName: String, timeout: Int ) extends BinRequest {
	def getType = Message.CAN_DO_TIMEOUT
	override protected def writeBody( out: DataOutputStream ) {
		out.write( funcName.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( timeout.toString.getBytes( "UTF-8") )
	}
}

class AllYours extends BinRequest {
	def getType = Message.ALL_YOURS
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class WorkExceptionReq( jobHandle: String, data: String ) extends BinRequest {
	def getType = Message.WORK_EXCEPTION
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class WorkExceptionRes( jobHandle: String, data: String ) extends BinResponse {
	def getType = Message.WORK_EXCEPTION
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class OptionReq( opt: String) extends BinRequest {
	def getType = Message.OPTION_REQ
	override protected def writeBody( out: DataOutputStream ) {
		out.write( opt.getBytes( "UTF-8") )
	}
}

case class OptionRes( opt: String) extends BinRequest {
	def getType = Message.OPTION_RES
	override protected def writeBody( out: DataOutputStream ) {
		out.write( opt.getBytes( "UTF-8") )
	}
}

case class WorkDataReq( jobHandle: String, data: String ) extends BinRequest {
	def getType = Message.WORK_DATA
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class WorkDataRes( jobHandle: String, data: String ) extends BinResponse {
	def getType = Message.WORK_DATA
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class WorkWarningReq( jobHandle: String, data: String ) extends BinRequest {
	def getType = Message.WORK_WARNING
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class WorkWarningRes( jobHandle: String, data: String ) extends BinResponse {
	def getType = Message.WORK_WARNING
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}


case class GrabJobUniq() extends BinRequest {
	def getType = Message.GRAB_JOB_UNIQ
	override protected def writeBody( out: DataOutputStream ) {
	}
}

case class JobAssignUniq( jobHandle: String, funcName: String, id: String, data: String ) extends BinResponse {
	def getType = Message.JOB_ASSIGN_UNIQ
	override protected def writeBody( out: DataOutputStream ) {
		out.write( jobHandle.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( funcName.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( id.getBytes( "UTF-8") )
		out.write( 0 )
		out.write( data.getBytes( "UTF-8") )
	}
}

case class SubmitJobHighBg(funcName: String, uniqueId: String, data: String) extends BinRequest {
	def getType = Message.SUBMIT_JOB_HIGH_BG
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}

case class SubmitJobLow(funcName: String, uniqueId: String, data: String) extends BinRequest {
	def getType = Message.SUBMIT_JOB_LOW
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}

case class SubmitJobLowBg(funcName: String, uniqueId: String, data: String) extends BinRequest {
	def getType = Message.SUBMIT_JOB_LOW_BG
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}

case class SubmitJobSched(funcName: String, uniqueId: String, minute: Int, hour: Int, day: Int, month: Int, weekDay: Int, data: String) extends BinRequest {
	def getType = Message.SUBMIT_JOB_SCHED
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write minute.toString.getBytes( "UTF-8")
		out write 0
		out write hour.toString.getBytes( "UTF-8")
		out write 0
		out write day.toString.getBytes( "UTF-8")
		out write 0
		out write month.toString.getBytes( "UTF-8")
		out write 0
		out write weekDay.toString.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}

case class SubmitJobEpoch(funcName: String, uniqueId: String, epoch: Long, data: String ) extends BinRequest {
	def getType = Message.SUBMIT_JOB_EPOCH
	override protected def writeBody( out: DataOutputStream ) {
		out write funcName.getBytes( "UTF-8")
		out write 0
		out write uniqueId.getBytes( "UTF-8")
		out write 0
		out write epoch.toString.getBytes( "UTF-8")
		out write 0
		out write data.getBytes( "UTF-8")
	}
}







