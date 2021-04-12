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



function Scheduler( params = {} ) {
	this.runners = params.runners || {} ;
	this.jobsUrl = params.jobsUrl || null ;
	this.everyMs = params.everyMs || 1000 ;
	this.localJobs = [] ;

	// RootsDB
	this.world = new rootsDb.World() ;
	this.jobs = null ;
	
	this.isInit = false ;
	this.isRunning = false ;
	this.timer = null ;
	
	this.init() ;
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
		lockTimeout: 60 * 1000 ,
		indexes: [
			{ properties: { runner: 1 } } ,
			{ properties: { at: 1 } }
		]
	} , Job.schema ) ;
	
	this.jobs = await this.world.createAndInitVersionCollection( 'jobs' , jobsDescriptor ) ;
} ;



Scheduler.prototype.start = async function() {
	if ( this.isRunning ) { return ; }
	if ( this.isInit ) { await this.init() ; }
	
	if ( this.timer ) {
		clearTimeout( this.timer ) ;
		this.timer = null ;
	}
	
	this.timer = setInterval( () => this.check() , this.everyMs ) ;

	this.isRunning = true ;
} ;



Scheduler.prototype.check = async function() {
	var job ,
		time = Date.now() ;
	
	for ( job of this.localJobs ) {
		if ( time >= job.at ) {
			// For instance we run it in sequence
			await job.run() ;
		}
	}
} ;



Scheduler.prototype.addJob = function( params = {} ) {
	var job = new Job( this , params ) ;
	this.localJobs.push( job ) ;
	return job ;
} ;

