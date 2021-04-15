/*
	Seventh

	Copyright (c) 2017 - 2020 Cédric Ronvel

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



/*
	Based on Seventh.Queue.
*/



const Promise = require( 'seventh' ) ;

const Logfella = require( 'logfella' ) ;
const log = Logfella.global.use( 'scheduler' ) ;



function Domain( scheduler , name , params = {} ) {
	this.scheduler = scheduler ;
	this.name = name ;
	this.runners = params.runners || {} ;
	this.runnerNames = Object.keys( this.runners ) ;
	this.concurrency = + params.concurrency || 1 ;
	
	this.jobs = new Map() ;			// all jobs
	this.pendingJobs = new Map() ;	// only pending jobs (not run)
	this.runningJobs = new Map() ;	// only running jobs (not done)
	this.errorJobs = new Map() ;	// jobs that have failed
	this.jobsDone = new Map() ;		// jobs that finished successfully

	// Internal
	this.isDomainRunning = true ;
	this.isLoopRunning = false ;
	this.isRetrieveLoopRunning = false ;
	this.ready = Promise.resolved ;

	// External API, resolved when there is no jobs anymore in the queue, a new Promise is created when new element are injected
	this.drained = Promise.resolved ;

	// External API, resolved when the Domain has nothing to do: either it's drained or the pending jobs have dependencies that cannot be solved
	this.idle = Promise.resolved ;
}

module.exports = Domain ;



Domain.prototype.setConcurrency = function( concurrency ) { this.concurrency = + concurrency || 1 ; } ;
Domain.prototype.stop = Domain.prototype.pause = function() { this.isDomainRunning = false ; } ;
Domain.prototype.has = function( id ) { return this.jobs.has( id ) ; } ;



Domain.prototype.run = Domain.prototype.resume = async function() {
	var job ;

	this.isDomainRunning = true ;

	if ( this.isLoopRunning ) { return ; }
	this.isLoopRunning = true ;

	// Adding jobs during this async loop would continue the loop
	for ( job of this.pendingJobs.values() ) {
		// This should be done synchronously:
		if ( this.idle.isSettled() ) { this.idle = new Promise() ; }

		// It's ready when the concurrent level is below the limit
		await this.ready ;

		// Check if something has stopped the queue while we were awaiting.
		// This check MUST be done only after "await", before is potentially synchronous, and things only change concurrently during an "await".
		if ( ! this.isDomainRunning ) { this.finishRun() ; return ; }

		this.runJob( job ) ;
	}

	this.finishRun() ;
} ;



// Finish current run
Domain.prototype.finishRun = function() {
	this.isLoopRunning = false ;

	if ( ! this.pendingJobs.size ) { this.drained.resolve() ; }
	if ( ! this.runningJobs.size ) { this.idle.resolve() ; }
} ;



Domain.prototype.retrieveLoop = async function() {
	if ( this.isRetrieveLoopRunning ) { return ; }
	this.isRetrieveLoopRunning = true ;

	for ( ;; ) {
		await this.retrieveJobs() ;
		if ( ! this.isDomainRunning ) { break ; }
		await Promise.resolveTimeout( 1000 ) ;
		if ( ! this.isDomainRunning ) { break ; }
	}

	this.isRetrieveLoopRunning = false ;
} ;



Domain.prototype.runJob = async function( job ) {
	// Immediately remove it synchronously from the pending queue and add it to the running one
	this.pendingJobs.delete( job.id ) ;
	this.runningJobs.set( job.id , job ) ;

	if ( this.runningJobs.size >= this.concurrency ) { this.ready = new Promise() ; }

	// Async part
	if ( await this.job.run( job.data ) ) {
		this.jobsDone.set( job.id , job ) ;
	}
	else {
		this.errorJobs.set( job.id , job ) ;
	}

	this.runningJobs.delete( job.id ) ;
	if ( this.runningJobs.size < this.concurrency ) { this.ready.resolve() ; }

	// This MUST come last
	if ( ! this.isLoopRunning ) {
		if ( this.isDomainRunning && this.pendingJobs.size ) { this.run() ; }
		else { this.finishRun() ; }
	}
} ;



Domain.prototype.addJob = function( job ) {
	// Don't add it twice!
	if ( this.jobs.has( job.id ) ) { return false ; }

	this.jobs.set( job.id , job ) ;
	this.pendingJobs.set( job.id , job ) ;
	if ( this.isDomainRunning && ! this.isLoopRunning ) { this.run() ; }
	if ( this.drained.isSettled() ) { this.drained = new Promise() ; }
} ;



Domain.prototype.retrieveJobs = async function() {
	var job , dbJob , dbJobs , query ;
	
	query = {
		scheduledFor: { $lte: new Date() } ,
		runner: { $in: this.runnerNames } ,
		status: { $in: [ 'pending' , 'error' ] }
	} ;

	/*
		We could add more conditions here, for example based on runner, or add a "domain" feature,
		so multiple instances of scheduler could run concurrently on the same DB back-end,
		each running only jobs of a kind.
	*/

	try {
		dbJobs = ( await this.jobs.lockingFind( query ) ).batch ;
	}
	catch ( error ) {
		// Log the error, but do nothing, just return
		log.error( "Can't retrieve jobs: %E" , error ) ;
		return ;
	}

	for ( dbJob of dbJobs ) {
		job = new Job( this , dbJob , { domain: domainName , dbObject: dbJob } ) ;
		this.addJob( job ) ;
		console.log( "Retrieved job:" , job ) ;
	}
} ;

