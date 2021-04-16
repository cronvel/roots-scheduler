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



const Job = require( './Job.js' ) ;
const Domain = require( './Domain.js' ) ;

const Promise = require( 'seventh' ) ;
const Ngev = require( 'nextgen-events' ) ;
const rootsDb = require( 'roots-db' ) ;

const Logfella = require( 'logfella' ) ;
const log = Logfella.global.use( 'scheduler' ) ;



function Scheduler( params = {} ) {
	this.domains = {} ;
	this.runners = {} ;
	this.extraRunnerArgs = Array.isArray( params.extraRunnerArgs ) ? params.extraRunnerArgs : [] ;
	this.jobsUrl = params.jobsUrl || null ;

	// RootsDB
	this.world = params.world || new rootsDb.World() ;
	this.jobs = params.jobs || null ;	// The RootsDB jobs collection

	// Default domain params
	this.concurrency = + params.concurrency || Infinity ;
	this.retrieveDelay = params.retrieveDelay ?? 1000 ;
	this.retryDelay = params.retryDelay ?? 30 * 1000 ;
	this.retryDelayExpBase = + params.retryDelayExpBase || 1 ;
	this.maxRetry = + params.maxRetry || 0 ;

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

// Expose class to userland
Scheduler.Job = Job ;
Scheduler.Domain = Domain ;



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

	if ( ! this.jobs ) {
		let jobsDescriptor = Object.assign( {} , Job.schema , {
			url: this.jobsUrl ,
			lockTimeout: 5 * 60 * 1000
		} ) ;

		this.jobs = await this.world.createAndInitCollection( 'jobs' , jobsDescriptor ) ;
	}
} ;



Scheduler.prototype.start = async function() {
	if ( this.isRunning ) { return ; }
	if ( ! this.isInit ) { await this.init() ; }

	this.isRunning = true ;

	for ( let domain of Object.values( this.domains ) ) { domain.run() ; }
} ;



// Add a job and save it to the DB.
// /!\ It is not loaded ATM, just pushed to the DB and will be retrieved on the next call to .retrieveDomainJobs().
Scheduler.prototype.addJob = function( params = {} ) {
	var job = new Job( this , params ) ,
		dbJob = this.jobs.createDocument( job.export() ) ;

	job.setRuntimeParams( { id: dbJob.getKey() , dbObject: dbJob } ) ;
	job.dbObject.save() ;
} ;



Scheduler.prototype.loggerHook = function( message , data ) {
	if ( data.hookData instanceof Job ) { data.hookData.loggerHook( message , data ) ; }
} ;

