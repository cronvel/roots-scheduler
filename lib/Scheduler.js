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

const Job = require( './Job.js' ) ;
const Domain = require( './Domain.js' ) ;

const Logfella = require( 'logfella' ) ;
const log = Logfella.global.use( 'scheduler' ) ;



function Scheduler( params = {} ) {
	this.domains = {} ;
	this.runners = {} ;
	this.concurrency = + params.concurrency || Infinity ;
	this.jobsUrl = params.jobsUrl || null ;
	this.period = params.period || 1000 ;

	// RootsDB
	this.world = new rootsDb.World() ;
	this.jobs = null ;
	
	this.isInit = false ;
	this.isRunning = false ;

	// Runner logger
	this.logger = new Logfella() ;
	this.logger.configure( {
		minLevel: 'debug' ,
		defaultDomain: 'scheduler'
	} ) ;
	this.logger.addTransport( 'hook' , { minLevel: 'trace' , hook: this.loggerHook.bind( this ) } ) ;
	
	if ( params.domains ) { this.createDomains( params.domains ) ; }
	if ( params.runners ) { this.createDomains( { default: { runners: params.runners } } ) ; }
}

Scheduler.prototype = Object.create( Ngev.prototype ) ;
Scheduler.prototype.constructor = Scheduler ;

module.exports = Scheduler ;



Scheduler.prototype.createDomains = function( domains ) {
	var domainName , domainParams , runnerName , domain ;
	
	for ( domainName in domains ) {
		domainParams = domains[ domainName ] ;
		domain = this.domains[ domainName ] = new Domain( this , domainName , domainParams ) ;
		
		for ( runnerName in domain.runners ) {
			this.runners[ runnerName ] = domain.runners[ runnerName ] ;
		}
	}
} ;



Scheduler.prototype.init = async function() {
	if ( this.isInit ) { return ; }
	this.isInit = true ;
	
	var jobsDescriptor = Object.assign( {
		url: this.jobsUrl ,
		canLock: true ,
		lockTimeout: 5 * 60 * 1000 ,
		indexes: [
			{ properties: { scheduledFor: 1 } } ,
			{ properties: { runner: 1 } } ,
			{ properties: { status: 1 } }
		]
	} , Job.schema ) ;
	
	this.jobs = await this.world.createAndInitCollection( 'jobs' , jobsDescriptor ) ;
} ;



Scheduler.prototype.start = async function() {
	if ( this.isRunning ) { return ; }
	if ( ! this.isInit ) { await this.init() ; }
	
	this.isRunning = true ;
	//Object.keys( this.domains ).forEach( domainName => this.runDomainJobs( domainName ) ) ;
	for ( let domain of this.domains.values() ) { domain.run() ; }
} ;



Scheduler.prototype.runDomainJobs = async function( domainName ) {
	var job , localJobs ,
		domain = this.domains[ domainName ] ;
	
	if ( domain.timer ) {
		clearTimeout( domain.timer ) ;
		domain.timer = null ;
	}
	
	await this.retrieveDomainJobs( domainName ) ;
	
	// Copy the jobs array, it can be modified during the async loop
	localJobs = [ ... domain.localJobs.values() ] ;

	for ( job of localJobs ) {
		if ( Date.now() < job.scheduledFor ) { continue ; }

		// For instance we run jobs one at a time
		await job.run() ;
		if ( job.status === 'done' ) { this.removeLocalJob( job ) ; }
	}

	domain.timer = setTimeout( () => this.runDomainJobs( domainName ) , this.period ) ;
} ;



Scheduler.prototype.retrieveDomainJobs = async function( domainName ) {
	var job , dbJob , dbJobs , query ,
		domain = this.domains[ domainName ] ;
	
	query = {
		scheduledFor: { $lte: new Date() } ,
		runner: { $in: domain.runnerNames } ,
		status: { $in: [ 'pending' , 'error' ] }
	} ;

	/*
		We could add more conditions here, for example based on runner, or add a "domain" feature,
		so multiple instances of scheduler could run concurrently on the same DB back-end,
		each running only jobs of a kind.
	*/

	dbJobs = ( await this.jobs.lockingFind( query ) ).batch ;
	
	for ( dbJob of dbJobs ) {
		if ( domain.localJobs.has( dbJob.id ) ) { continue ; }
		
		job = new Job( this , dbJob , { domain: domainName , dbObject: dbJob } ) ;
		domain.localJobs.set( job.id , job ) ;
		console.log( "Retrieved job:" , job ) ;
	}
} ;



// Add a job and save it to the DB.
// /!\ It is not loaded ATM, just pushed to the DB and will be retrieved on the next call to .retrieveDomainJobs().
Scheduler.prototype.addJob = function( params = {} ) {
	var job = new Job( this , params ) ,
		dbJob = this.jobs.createDocument( job.export() ) ;

	job.setRuntimeParams( { id: dbJob.getKey() , dbObject: dbJob } ) ;
	job.dbObject.save() ;
} ;



// DEPRECATED
// But contains things that should be done by .addJob()
Scheduler.prototype.addLocalJob = function( params = {} ) {
	var job = new Job( this , params ) ;
	job.dbObject = this.jobs.createDocument( job.export() ) ;
	this.localJobs.set( job.dbObject.getKey() , job ) ;
	job.dbObject.lock() ;	// Lock it immediately
	job.sync() ;

	return job ;
} ;



Scheduler.prototype.removeLocalJob = function( job ) {
	this.domains[ job.domain ].localJobs.delete( job.dbObject.getKey() ) ;
} ;



Scheduler.prototype.loggerHook = function( message , data ) {
	if ( data.hookData instanceof Job ) { data.hookData.loggerHook( message , data ) ; }
} ;

