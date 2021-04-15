#!/usr/bin/env node
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



const Scheduler = require( '..' ) ;
const Promise = require( 'seventh' ) ;



var runners = {
	mail: async ( data , job ) => {
		console.log( ">>> Mail: " , data ) ;
		job.debug( "Start at %s" , new Date() ) ;
		await Promise.resolveTimeout( 2000 ) ;
		job.info( "Middle at %s" , new Date() ) ;
		await Promise.resolveTimeout( 2000 ) ;
		job.verbose( "Done at %s" , new Date() ) ;
		console.log( "<<< Mailed: " , data ) ;
	} ,
	error: async ( data , job ) => {
		console.log( ">>> Error runner: " , data ) ;
		await Promise.resolveTimeout( 2000 ) ;
		var error = new Error( "Dang!" ) ;
		job.error( "Error: %E" , error ) ;
		console.log( "<<< Error throwed!" ) ;
		throw error ;
	}
} ;

var scheduler = new Scheduler( {
	runners ,
	jobsUrl: 'mongodb://localhost:27017/scheduler/jobs' ,
	maxRetry: 3 ,
	retryDelay: 10 * 1000 ,
	retryDelayExpBase: 2
} ) ;



async function run() {
	await scheduler.start() ;
	scheduler.addJob( { runner: 'mail' , scheduledFor: Date.now() + 500 , data: { to: 'bill@bill.com' } } ) ;
	scheduler.addJob( { runner: 'error' , scheduledFor: Date.now() + 4000 } ) ;
}

run() ;

