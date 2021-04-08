/*
	Roots Scheduler

	Copyright (c) 2020 - 2021 Cédric Ronvel

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



function Job( queue , fn , data , tasks ) {
	this.queue = queue ;
	this.fn = fn ;
	this.data = data ;
	this.status = Job.PENDING ;
	this.promise = null ;
	this.tasks = {} ;
	this.progress = null ;
	this.value = null ;
	this.error = null ;
	this.failCount = 0 ;
	this.fatal = false ;

	// Init all tasks to false
	if ( Array.isArray( tasks ) ) {
		tasks.forEach( task => this.tasks[ task ] = false ) ;
	}
}

module.exports = Job ;



Job.PENDING = 0 ;
Job.RUNNING = 1 ;
Job.DONE = 2 ;
Job.ERROR = -1 ;



Job.prototype.run = function() {
	var fn = typeof this.fn === 'string' ? this.queue.functions[ this.fn ] : this.fn ;

	if ( typeof fn !== 'function' ) {
		this.status = Job.ERROR ;
		this.failCount ++ ;
		this.fatal = true ;	// this is an unrecoverable error
		this.error = new Error( "Cannot find a function for: " + this.fn ) ;
		this.queue.emit( 'jobError' , this ) ;
		return Promise.reject( this.error ) ;
	}

	this.status = Job.RUNNING ;
	this.queue.emit( 'jobStart' , this ) ;

	// Ensure that a promise is returned
	this.promise = Promise.resolve( fn( this.data , this ) ) ;

	this.promise.then(
		value => {
			this.status = Job.DONE ;
			this.value = value ;
			this.queue.emit( 'jobDone' , this ) ;
		} ,
		error => {
			this.status = Job.ERROR ;
			this.error = error ;
			this.failCount ++ ;
			this.queue.emit( 'jobError' , this ) ;
		}
	) ;

	return this.promise ;
} ;



Job.prototype.isTaskDone = function( task ) {
	return this.tasks[ task ] === true ;
} ;



Job.prototype.isTaskPending = function( task ) {
	return this.tasks[ task ] === false ;
} ;



Job.prototype.taskDone = function( task ) {
	if ( this.tasks[ task ] === false ) {
		this.tasks[ task ] = true ;
		this.queue.progress() ;
	}
} ;



/*
Job.prototype.retry = function() {
	if ( this.status !== Job.ERROR || this.fatal ) { return false ; }

	this.status = Job.PENDING ;
	this.promise = null ;
	this.error = null ;

	return true ;
} ;
*/



Job.prototype.toObject = function() {
	var object = Object.assign( {} , this ) ;
	delete object.queue ;
	delete object.promise ;
	return object ;
} ;



Job.restoreFromObject = function( queue , object ) {
	var job = new Job( queue ) ;

	Object.assign( job , object ) ;

	// We are restoring, so previously running jobs are now pending
	if ( job.status === Job.RUNNING ) { job.status = Job.PENDING ; }

	return job ;
} ;

