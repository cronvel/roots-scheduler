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
const Ngev = require( 'nextgen-events' ) ;
const rootsDb = require( 'roots-db' ) ;
const Logfella = require( 'logfella' ) ;

const Job = require( './Job.js' ) ;



function Scheduler( params = {} ) {
	this.runners = params.runners || {} ;
	this.jobsUrl = params.jobsUrl || null ;
	this.period = params.period || 1000 ;

	this.localJobs = new Map() ;

	// RootsDB
	this.world = new rootsDb.World() ;
	this.jobs = null ;
	
	this.isInit = false ;
	this.isRunning = false ;
	this.timer = null ;

	// Runner logger
	this.logger = new Logfella() ;
	this.logger.configure( {
		minLevel: 'debug' ,
		defaultDomain: 'scheduler'
	} ) ;
	this.logger.addTransport( 'hook' , { minLevel: 'trace' , hook: this.loggerHook.bind( this ) } ) ;
}

Scheduler.prototype = Object.create( Ngev.prototype ) ;
Scheduler.prototype.constructor = Scheduler ;

module.exports = Scheduler ;



Scheduler.prototype.init = async function() {
	if ( this.isInit ) { return ; }
	this.isInit = true ;
	
	var jobsDescriptor = Object.assign( {
		url: this.jobsUrl ,
		canLock: true ,
		lockTimeout: 5 * 60 * 1000 ,
		indexes: [
			{ properties: { runner: 1 } } ,
			{ properties: { scheduledFor: 1 } }
		]
	} , Job.schema ) ;
	
	this.jobs = await this.world.createAndInitCollection( 'jobs' , jobsDescriptor ) ;
} ;



Scheduler.prototype.start = async function() {
	if ( this.isRunning ) { return ; }
	if ( ! this.isInit ) { await this.init() ; }
	
	this.isRunning = true ;
	this.runJobs() ;
} ;



Scheduler.prototype.runJobs = async function() {
	var job , localJobs ;
	
	if ( this.timer ) {
		clearTimeout( this.timer ) ;
		this.timer = null ;
	}
	
	await this.retrieveJobs() ;
	
	// Copy the jobs array, it can be modified during the async loop
	localJobs = [ ... this.localJobs.values() ] ;

	for ( job of localJobs ) {
		if ( Date.now() < job.scheduledFor ) { continue ; }

		// For instance we run jobs one at a time
		await job.run() ;
		if ( job.status === 'done' ) { this.removeLocalJob( job ) ; }
	}

	this.timer = setTimeout( () => this.runJobs() , this.period ) ;
} ;



Scheduler.prototype.addJob = function( params = {} ) {
	var job = new Job( this , params ) ;
	job.dbObject = this.jobs.createDocument( job.export() ) ;
	this.localJobs.set( job.dbObject.getKey() , job ) ;
	job.dbObject.lock() ;	// Lock it immediately
	job.sync() ;

	return job ;
} ;



Scheduler.prototype.retrieveJobs = async function() {
	var job , dbJob , dbJobs , query ;
	
	query = {
		scheduledFor: { $lte: new Date() } ,
		status: { $in: [ 'pending' , 'error' ] }
	} ;

	/*
		We could add more conditions here, for example based on runner, or add a "domain" feature,
		so multiple instances of scheduler could run concurrently on the same DB back-end,
		each running only jobs of a kind.
	*/

	dbJobs = ( await this.jobs.lockingFind( query ) ).batch ;
	
	for ( dbJob of dbJobs ) {
		if ( this.localJobs.has( dbJob.getKey() ) ) { continue ; }
		
		job = new Job( this , dbJob ) ;
		job.dbObject = dbJob ;
		this.localJobs.set( dbJob.getKey() , job ) ;
		console.log( "Retrieved job:" , job ) ;
	}
} ;



Scheduler.prototype.removeLocalJob = function( job ) {
	this.localJobs.delete( job.dbObject.getKey() ) ;
} ;



Scheduler.prototype.loggerHook = function( message , data ) {
	if ( data.hookData instanceof Job ) { data.hookData.loggerHook( message , data ) ; }
} ;

