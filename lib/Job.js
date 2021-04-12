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
function Job( scheduler , params ) {
	this.scheduler = scheduler ;
	this.runner = params.runner ;	// a runner (function) ID
	this.scheduledFor = ! params.scheduledFor ? new Date() :
		params.scheduledFor instanceof Date ? params.scheduledFor :
		new Date( params.scheduledFor ) ;
	this.data = params.data && typeof params.data === 'object' && ! Array.isArray( params.data ) ? params.data : null ;
	this.tasks = {} ;

	// It can't be running since we are creating it (even if it's a restore)
	this.status = params.status && params.status !== 'pending' && Job.STATUSES.has( params.status ) ? params.status : 'pending' ;
	this.startedAt = null ;	// the date-time when the job was started (for the last time in case of retry)
	this.duration = null ;	// the duration of job (for the last time in case of retry)

	this.failCount = params.failCount || 0 ;
	this.lastError = params.lastError && params.lastError !== 'pending' ? params.lastError : null ;

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
}

module.exports = Job ;



Job.STATUSES = new Set( [
	'pending' ,		// pending, not used
	'taken' ,		// a worker has taken this job, but has not start working on it
	'running' ,		// a worker has taken this job and is currently running it
	'done' ,		// the job is done
	'error' ,		// the job has aborted with an error, the error is not necessary permanent, it can be retried
	'fatal'			// the job has aborted with an error which is fatal, it should not be retried because this is a permanent error
] ) ;



// Export for the DB
Job.prototype.export = function( into = null ) {
	var toExport = {
		runner: this.runner ,
		scheduledFor: this.scheduledFor ,
		data: this.data ,
		status: this.status ,
		startedAt: this.startedAt ,
		duration: this.duration ,
		tasks: this.tasks ,
		failCount: this.failCount ,
		lastError: this.lastError ? Object.assign( {} , this.lastError ) : null
	} ;

	if ( into ) { return Object.assign( into , toExport ) ; }

	return toExport ;
} ;



Job.schema = {
	properties: {
		runner: { type: 'string' } ,
		scheduledFor: { type: 'date' , sanitize: 'toDate' } ,
		data: { type: 'strictObject' , optional: true } ,
		status: { type: 'string' , in: [ ... Job.STATUSES ] } ,
		startedAt: { type: 'date' , sanitize: 'toDate' , optional: true } ,
		duration: { type: 'number' , optional: true } ,
		tasks: { type: 'strictObject' , of: { type: 'boolean' } } ,
		failCount: { type: 'integer' } ,
		lastError: { type: 'object' , optional: true }
	}
} ;



Job.prototype.run = async function() {
	var runnerFn = this.scheduler.runners[ this.runner ] ;

	if ( this.status === 'running' || this.status === 'done' ) { return ; }

	this.status = 'running' ;
	this.startedAt = new Date() ;
	this.duration = null ;
	this.sync() ;	// await?
	this.scheduler.emit( 'jobStart' , this ) ;

	var runnerApi = {
		isTaskDone: this.isTaskDone.bind( this ) ,
		isTaskPending: this.isTaskPending.bind( this ) ,
		taskDone: this.taskDone.bind( this )
	} ;

	try {
		await runnerFn( this.data , runnerApi ) ;
	}
	catch ( error ) {
		this.status = 'error' ;
		this.lastError = error ;
		this.failCount ++ ;
		this.duration = Date.now() - this.startedAt ;
		this.sync() ;	// await?
		this.scheduler.emit( 'jobError' , this ) ;
		return ;
	}

	this.status = 'done' ;
	this.duration = Date.now() - this.startedAt ;
	this.sync() ;	// await?
	this.scheduler.emit( 'jobDone' , this ) ;
} ;



// Sync with the DB
Job.prototype.sync = function() {
	if ( ! this.dbObject ) { return ; }
	this.export( this.dbObject ) ;
	return this.dbObject.save() ;
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

