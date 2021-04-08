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



function Job( scheduler , runner , data , tasks ) {
	this.scheduler = scheduler ;
	this.runner = runner ;	// a runner (function) ID
	this.data = data ;
	this.status = 'pending' ;
	this.tasks = {} ;

	this.failCount = 0 ;
	this.fatal = false ;
	this.lastError = null ;

	this.promise = null ;

	// Init all tasks to false
	if ( Array.isArray( tasks ) ) {
		tasks.forEach( task => this.tasks[ task ] = false ) ;
	}
	else if ( tasks && typeof tasks === 'object' ) {
		for ( let key in tasks ) {
			this.tasks[ key ] = !! tasks[ key ] ;
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
	var job = new Job( scheduler , object.runner , object.data , object.tasks ) ;

	// We are restoring, so previously running jobs are now pending
	job.status = object.status === 'running' ? 'pending' : object.status ;

	job.failCount = + object.failCount ;
	job.fatal = !! object.fatal ;
	job.lastError = object.lastError ;

	return job ;
} ;

