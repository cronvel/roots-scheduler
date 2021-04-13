#!/usr/bin/env node

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
		throw error ;
	}
} ;

var scheduler = new Scheduler( {
	runners ,
	jobsUrl: 'mongodb://localhost:27017/scheduler/jobs'
} ) ;



async function run() {
	await scheduler.start() ;
	scheduler.addJob( { runner: 'mail' , scheduledFor: Date.now() + 500 , data: { to: 'bill@bill.com' } } ) ;
	scheduler.addJob( { runner: 'error' , scheduledFor: Date.now() + 4000 } ) ;
}

run() ;

