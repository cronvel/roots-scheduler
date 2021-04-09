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



function Job( scheduler , params ) {
	this.scheduler = scheduler ;
	this.runner = params.runner ;	// a runner (function) ID
	this.data = params.data || null ;
	this.tasks = {} ;
	this.at = params.at || Date.now() ;

	// It can't be running since we are creating it (even if it's a restore)
	this.status = params.status && params.status !== 'pending' ? params.status : 'pending' ;

	this.failCount = 0 ;
	this.fatal = false ;
	this.lastError = null ;

	this.promise = null ;

	// Init all tasks to false
	if ( Array.isArray( params.tasks ) ) {
		params.tasks.forEach( task => this.tasks[ task ] = false ) ;
	}
	else if ( params.tasks && typeof params.tasks === 'object' ) {
		for ( let key in params.tasks ) {
			this.tasks[ key ] = !! params.tasks[ key ] ;
		}
	}
}

module.exports = Job ;



Job.prototype.run = function() {
	var runnerFn = this.scheduler.runners[ this.runner ] ;

	this.status = 'running' ;
	this.scheduler.emit( 'jobStart' , this ) ;

	var runnerApi = {
		isTaskDone: this.isTaskDone.bind( this ) ,
		isTaskPending: this.isTaskPending.bind( this ) ,
		taskDone: this.taskDone.bind( this )
	} ;

	// Ensure that a promise is returned
	this.promise = Promise.resolve( runnerFn( this.data , runnerApi ) ) ;

	this.promise.then(
		() => {
			this.status = 'done' ;
			this.scheduler.emit( 'jobDone' , this ) ;
		} ,
		error => {
			this.status = 'error' ;
			this.lastError = error ;
			this.failCount ++ ;
			this.scheduler.emit( 'jobError' , this ) ;
		}
	) ;

	return this.promise ;
} ;



// API for runner
Job.prototype.isTaskDone = function( task ) { return this.tasks[ task ] === true ; } ;
Job.prototype.isTaskPending = function( task ) { return this.tasks[ task ] === false ; } ;

Job.prototype.taskDone = function( task ) {
	if ( this.tasks[ task ] === false ) {
		this.tasks[ task ] = true ;
		this.scheduler.progress() ;
	}
} ;



// Export for the DB
Job.prototype.export = function() {
	return {
		runner: this.runner ,
		data: this.data ,
		status: this.status ,
		tasks: this.tasks ,
		failCount: this.failCount ,
		fatal: this.fatal ,
		lastError: '' + this.lastError
	} ;
} ;



// Create from a plain object export
Job.createFromExport = function( scheduler , object ) {
	var job = new Job( scheduler , object ) ;

	if ( job.status === 'running' ) { job.status = 'pending' ; }

	return job ;
} ;

