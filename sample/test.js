#!/usr/bin/env node

"use strict" ;



const Scheduler = require( '..' ) ;



var runners = {
	mail: data => {
		console.log( "Mail: " , data ) ;
	}
} ;

var scheduler = new Scheduler( {
	runners ,
	jobsUrl: 'mongodb://localhost:27017/scheduler/jobs'
} ) ;



async function run() {
	await scheduler.start() ;
	scheduler.addJob( { runner: 'mail' , at: Date.now() + 1000 , data: { to: 'bob@bob.com' } } ) ;
}

run() ;

