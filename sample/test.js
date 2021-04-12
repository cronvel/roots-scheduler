#!/usr/bin/env node

"use strict" ;



const Scheduler = require( '..' ) ;
const Promise = require( 'seventh' ) ;



var runners = {
	mail: async ( data ) => {
		console.log( ">>> Mail: " , data ) ;
		await Promise.resolveTimeout( 4000 ) ;
		console.log( "<<< Mailed: " , data ) ;
	}
} ;

var scheduler = new Scheduler( {
	runners ,
	jobsUrl: 'mongodb://localhost:27017/scheduler/jobs'
} ) ;



async function run() {
	await scheduler.start() ;
	scheduler.addJob( { runner: 'mail' , scheduledFor: Date.now() + 4000 , data: { to: 'bob@bob.com' } } ) ;
}

run() ;

