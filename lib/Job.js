/*
	Roots Scheduler

	Copyright (c) 2021 Cédric Ronvel

	The MIT License (MIT)

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

"use strict" ;



const Promise = require( 'seventh' ) ;



// The constructor can be used when importing from DB, so all exportable properties should be importable as well
function Job( scheduler , params = {} , runtimeParams = null ) {
	this.scheduler = scheduler ;
	this.id = params.id || null ;
	this.runner = params.runner ;	// a runner (function) ID
	this.domain = params.domain || null ;	// the domain of the job, not saved to the DB, it's purely runtime and based on the runner
	this.data = params.data && typeof params.data === 'object' && ! Array.isArray( params.data ) ? params.data : null ;
	this.scheduledOn = ! params.scheduledOn ? new Date() :
		params.scheduledFor instanceof Date ? params.scheduledFor :
		new Date( params.scheduledFor ) ;
	this.scheduledFor = ! params.scheduledFor ? new Date() :
		params.scheduledFor instanceof Date ? params.scheduledFor :
		new Date( params.scheduledFor ) ;
	this.tasks = {} ;

	// It can't be running since we are creating it (even if it's a restore)
	this.status = params.status && params.status !== 'pending' && Job.STATUSES.has( params.status ) ? params.status : 'pending' ;
	this.startedAt = null ;	// the date-time when the job was started (for the last time in case of retry)
	this.endedAt = null ;	// the date-time when the job was finished (for the last time in case of retry)
	this.duration = null ;	// the duration of job (for the last time in case of retry)

	this.failCount = params.failCount || 0 ;
	
	this.runs = Array.isArray( params.runs ) ? params.runs : [] ;

	// Init all tasks to false
	if ( Array.isArray( params.tasks ) ) {
		params.tasks.forEach( task => this.tasks[ task ] = false ) ;
	}
	else if ( params.tasks && typeof params.tasks === 'object' ) {
		for ( let key in params.tasks ) {
			this.tasks[ key ] = !! params.tasks[ key ] ;
		}
	}
	
	this.dbObject = null ;

	if ( runtimeParams && typeof runtimeParams === 'object' ) { this.setRuntimeParams( runtimeParams ) ; }
	
	if ( ! this.id ) { this.id = 'rnd' + this.runner + ( '' + Math.random() ).slice( 2 ) ; }
}

module.exports = Job ;



Job.STATUSES = new Set( [
	'pending' ,		// pending, not used
	'running' ,		// a worker has taken this job and is currently running it
	'done' ,		// the job is done
	'error' ,		// the job has aborted with an error, the error is not necessary permanent, it can be retried
	'fatal'			// the job has aborted with an error which is fatal, it should not be retried because this is a permanent error
] ) ;



Job.prototype.setRuntimeParams = function( params = {} ) {
	if ( params.dbObject ) { this.dbObject = params.dbObject ; }
	if ( params.domain ) { this.domain = params.domain ; }

	if ( params.id ) {
		this.id = '' + params.id ;
		if ( this.dbObject ) { this.dbObject.id = this.id ; }
	}
} ;



// Export for the DB
Job.prototype.export = function( into = null ) {
	var toExport = {
		id: this.id ,
		runner: this.runner ,
		data: this.data ,
		scheduledOn: this.scheduledOn ,
		scheduledFor: this.scheduledFor ,
		status: this.status ,
		startedAt: this.startedAt ,
		endedAt: this.endedAt ,
		duration: this.duration ,
		tasks: this.tasks ,
		failCount: this.failCount ,
		runs: this.runs
	} ;

	if ( into ) { return Object.assign( into , toExport ) ; }

	return toExport ;
} ;



Job.schema = {
	properties: {
		id: { type: 'string' } ,
		runner: { type: 'string' } ,
		data: { type: 'strictObject' , optional: true } ,
		scheduledOn: { type: 'date' , sanitize: 'toDate' } ,
		scheduledFor: { type: 'date' , sanitize: 'toDate' } ,
		status: { type: 'string' , in: [ ... Job.STATUSES ] } ,
		startedAt: { type: 'date' , sanitize: 'toDate' , optional: true } ,
		endedAt: { type: 'date' , sanitize: 'toDate' , optional: true } ,
		duration: { type: 'number' , optional: true } ,
		tasks: { type: 'strictObject' , of: { type: 'boolean' } } ,
		failCount: { type: 'integer' } ,
		runs: {
			type: 'array' ,
			of: {
				properties: {
					startedAt: { type: 'date' , sanitize: 'toDate' , optional: true } ,
					endedAt: { type: 'date' , sanitize: 'toDate' , optional: true } ,
					duration: { type: 'number' , optional: true } ,
					error: { type: 'object' , optional: true } ,
					logs: { type: 'array' , of: { type: 'string' } }
				}
			}
		}
	}
} ;



Job.prototype.run = async function() {
	var runData = { startedAt: null , endedAt: null , duration: null , error: null , logs: [] } ,
		runnerFn = this.scheduler.runners[ this.runner ] ;

	if ( this.status === 'running' || this.status === 'done' ) { return ; }

	this.status = 'running' ;
	this.startedAt = runData.startedAt = new Date() ;
	this.endedAt = runData.endedAt = null ;
	this.duration = runData.duration = null ;
	this.runs.push( runData ) ;
	this.sync() ;	// await?
	this.scheduler.emit( 'jobStart' , this ) ;

	try {
		await runnerFn( this.data , this ) ;
	}
	catch ( error ) {
		this.status = 'error' ;
		this.failCount ++ ;
		this.endedAt = runData.endedAt = new Date() ;
		this.duration = runData.duration = this.endedAt - this.startedAt ;
		runData.error = Job.errorToObject( error ) ;
		this.sync() ;	// await?
		this.scheduler.emit( 'jobError' , this ) ;
		return false ;
	}

	this.status = 'done' ;
	this.endedAt = runData.endedAt = new Date() ;
	this.duration = runData.duration = this.endedAt - this.startedAt ;
	this.sync() ;	// await?
	this.scheduler.emit( 'jobDone' , this ) ;
	return true ;
} ;



// Sync with the DB
Job.prototype.sync = function() {
	if ( ! this.dbObject ) { return ; }
	this.export( this.dbObject ) ;
	return this.dbObject.save() ;
} ;



Job.errorToObject = error => {
	var object = {
		name: error.name ,
		message: error.message ,
		stack: '' + error.stack
	} ;

	if ( error.code ) { object.code = error.code ; }

	return object ;
} ;



// Hook for logs, we simply add it to thelogs of the current run
Job.prototype.loggerHook = function( message , data ) {
	if ( this.runs.length ) { this.runs[ this.runs.length - 1 ].logs.push( message ) ; }
} ;



// API for runner
Job.prototype.isTaskDone = function( task ) { return this.tasks[ task ] === true ; } ;
Job.prototype.isTaskPending = function( task ) { return this.tasks[ task ] === false ; } ;



Job.prototype.taskDone = function( task ) {
	if ( this.tasks[ task ] === false ) {
		this.tasks[ task ] = true ;
		this.scheduler.progress() ;
		this.sync() ;
	}
} ;



Job.prototype.log = function( level , format , ... args ) {
	this.scheduler.logger.log( level , 'runner' , { hookData: this } , format , ... args ) ;
} ;

Job.prototype.trace = function( ... args ) { this.log( 'trace' , ... args ) ; } ;
Job.prototype.debug = function( ... args ) { this.log( 'debug' , ... args ) ; } ;
Job.prototype.verbose = function( ... args ) { this.log( 'verbose' , ... args ) ; } ;
Job.prototype.info = function( ... args ) { this.log( 'info' , ... args ) ; } ;
Job.prototype.warning = function( ... args ) { this.log( 'warning' , ... args ) ; } ;
Job.prototype.error = function( ... args ) { this.log( 'error' , ... args ) ; } ;
Job.prototype.fatal = function( ... args ) { this.log( 'fatal' , ... args ) ; } ;
Job.prototype.hdebug = function( ... args ) { this.log( 'hdebug' , ... args ) ; } ;

